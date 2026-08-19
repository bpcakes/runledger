use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

const PROCESS_OWNER_LABEL: &str = "org.runledger.test-process";
const PROCESS_OWNER_ENV: &str = "RUNLEDGER_TEST_CONTAINER_OWNER";
const PROBE_CHILD_ENV: &str = "RUNLEDGER_TEST_CONTAINER_LIFECYCLE_PROBE";
const DOCKER_CLI_ENV: &str = "RUNLEDGER_TEST_DOCKER_CLI";
const TEST_ADMIN_DATABASE_URL_ENV: &str = "RUNLEDGER_TEST_ADMIN_DATABASE_URL";
const TEST_PG_IMAGE_ENV: &str = "RUNLEDGER_TEST_PG_IMAGE";
const READY_MARKER: &str = "runledger-container-lifecycle-ready";
const DEFAULT_POSTGRES_IMAGE: &str = "postgres:18";
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(15);
const CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DOCKER_CLI_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const PROBE_READY_TIMEOUT: Duration = Duration::from_secs(60);

static PROBE_COUNTER: AtomicU64 = AtomicU64::new(1);
// Each parent test starts its own PostgreSQL container. Keep those probes from
// overwhelming a constrained shared Docker daemon in CI.
static LIFECYCLE_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn shared_container_is_removed_after_normal_process_exit() {
    let _guard = lifecycle_test_guard();
    if docker_lifecycle_assertions_are_unavailable() {
        return;
    }

    let mut probe = ProbeProcess::spawn();
    let container_id = probe.assert_ready();

    probe.finish_normally();
    assert_container_removed(&probe.owner, &container_id);
}

#[test]
fn shared_container_is_removed_after_forced_process_termination() {
    let _guard = lifecycle_test_guard();
    if docker_lifecycle_assertions_are_unavailable() {
        return;
    }

    let mut probe = ProbeProcess::spawn();
    let container_id = probe.assert_ready();

    probe.terminate_forcibly();
    assert_container_removed(&probe.owner, &container_id);
}

#[test]
fn shared_container_starts_without_a_docker_cli_for_the_optional_reaper() {
    let _guard = lifecycle_test_guard();
    if docker_lifecycle_assertions_are_unavailable() {
        return;
    }

    let mut probe = ProbeProcess::spawn_with_reaper_cli("runledger-missing-docker-cli");
    probe.assert_database_ready();
    probe.finish_normally();
}

#[test]
fn shared_container_starts_when_optional_reaper_cli_hangs() {
    let _guard = lifecycle_test_guard();
    if docker_lifecycle_assertions_are_unavailable() {
        return;
    }

    let script = TemporaryFile::new(
        std::env::temp_dir().join(format!("runledger-hanging-docker-cli-{}", unique_owner())),
    );
    std::fs::write(script.path(), "#!/bin/sh\nsleep 60\n").expect("write hanging Docker CLI probe");
    let mut permissions = std::fs::metadata(script.path())
        .expect("read hanging Docker CLI metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(script.path(), permissions)
        .expect("make hanging Docker CLI executable");

    let mut probe = ProbeProcess::spawn_with_reaper_cli(
        script
            .path()
            .to_str()
            .expect("temporary Docker CLI path must be UTF-8"),
    );
    probe.assert_database_ready();
    probe.finish_normally();
}

#[test]
#[ignore = "helper entrypoint launched explicitly by lifecycle parent tests"]
fn lifecycle_probe_child() {
    if std::env::var_os(PROBE_CHILD_ENV).is_none() {
        return;
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build lifecycle probe runtime");

    runtime.block_on(async {
        let (pool, database) = setup_ephemeral_pool("container_lifecycle_probe", 2).await;
        let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL server_version");
        let server_version_num =
            sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
                .fetch_one(&pool)
                .await
                .expect("read PostgreSQL server_version_num");

        println!("{READY_MARKER}\t{server_version_num}\t{server_version}");
        std::io::stdout()
            .flush()
            .expect("flush lifecycle readiness marker");

        tokio::task::spawn_blocking(|| {
            let mut command = String::new();
            std::io::stdin().read_line(&mut command)
        })
        .await
        .expect("join lifecycle probe stdin task")
        .expect("read lifecycle probe stdin");

        teardown_ephemeral_pool(pool, database).await;
    });
}

struct TemporaryFile(std::path::PathBuf);

impl TemporaryFile {
    fn new(path: std::path::PathBuf) -> Self {
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct ProbeProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    output: mpsc::Receiver<Result<String, String>>,
    captured_output: String,
    owner: String,
    expected_image: String,
}

impl ProbeProcess {
    fn spawn() -> Self {
        Self::spawn_with_optional_reaper_cli(None)
    }

    fn spawn_with_reaper_cli(docker_cli: &str) -> Self {
        Self::spawn_with_optional_reaper_cli(Some(docker_cli))
    }

    fn spawn_with_optional_reaper_cli(docker_cli: Option<&str>) -> Self {
        let owner = unique_owner();
        let expected_image =
            std::env::var(TEST_PG_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_POSTGRES_IMAGE.to_owned());
        let mut command =
            Command::new(std::env::current_exe().expect("resolve lifecycle test binary"));
        command
            .arg("--exact")
            .arg("lifecycle_probe_child")
            .arg("--ignored")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(PROBE_CHILD_ENV, "1")
            .env(PROCESS_OWNER_ENV, &owner)
            .env("TESTCONTAINERS_COMMAND", "remove")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(docker_cli) = docker_cli {
            command.env(DOCKER_CLI_ENV, docker_cli);
        }
        let mut child = command.spawn().expect("spawn lifecycle probe child");
        let stdin = child.stdin.take().expect("pipe lifecycle probe stdin");
        let stdout = child.stdout.take().expect("pipe lifecycle probe stdout");
        let stderr = child.stderr.take().expect("pipe lifecycle probe stderr");
        let (output_sender, output) = mpsc::channel();
        spawn_probe_output_reader("stdout", stdout, output_sender.clone());
        spawn_probe_output_reader("stderr", stderr, output_sender);

        Self {
            child,
            stdin: Some(stdin),
            output,
            captured_output: String::new(),
            owner,
            expected_image,
        }
    }

    fn assert_ready(&mut self) -> String {
        self.assert_database_ready();

        let container_ids = owned_container_ids(&self.owner);
        assert_eq!(
            container_ids.len(),
            1,
            "expected one lifecycle probe container for owner {}, found {container_ids:?}",
            self.owner
        );
        let container_id = container_ids[0].clone();
        let state = docker_output([
            "container",
            "inspect",
            "--format",
            "{{.Config.Image}}\t{{.State.Running}}",
            &container_id,
        ]);
        assert_eq!(
            state.trim(),
            format!("{}\ttrue", self.expected_image),
            "lifecycle probe container must use the configured PostgreSQL image and be running"
        );
        container_id
    }

    fn assert_database_ready(&mut self) {
        let deadline = Instant::now() + PROBE_READY_TIMEOUT;
        let ready_line = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = match self.output.recv_timeout(remaining) {
                Ok(Ok(line)) => line,
                Ok(Err(error)) => panic!(
                    "failed to read lifecycle probe output ({error}):\n{}",
                    self.captured_output
                ),
                Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                    "lifecycle probe did not become ready within {PROBE_READY_TIMEOUT:?}:\n{}",
                    self.captured_output
                ),
                Err(mpsc::RecvTimeoutError::Disconnected) => panic!(
                    "lifecycle probe exited before becoming ready:\n{}",
                    self.captured_output
                ),
            };
            self.captured_output.push_str(&line);

            if let Some(marker_index) = line.find(READY_MARKER) {
                break line[marker_index..].to_owned();
            }
        };

        let mut fields = ready_line.trim().splitn(3, '\t');
        assert_eq!(fields.next(), Some(READY_MARKER));
        let server_version_num = fields
            .next()
            .expect("read lifecycle server_version_num")
            .parse::<i32>()
            .expect("parse lifecycle server_version_num");
        let server_version = fields.next().expect("read lifecycle server_version");
        assert!(
            server_version_num >= 180_000,
            "lifecycle test requires PostgreSQL 18+, got {server_version} ({server_version_num})"
        );
        eprintln!(
            "lifecycle probe PostgreSQL server_version={server_version}, server_version_num={server_version_num}"
        );
    }

    fn finish_normally(&mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("wait for lifecycle probe child");
        self.capture_remaining_output();
        assert!(
            status.success(),
            "lifecycle probe did not exit normally ({status}):\n{}",
            self.captured_output
        );
    }

    fn terminate_forcibly(&mut self) {
        self.child
            .kill()
            .expect("forcibly terminate lifecycle probe child");
        let status = self
            .child
            .wait()
            .expect("wait for terminated lifecycle probe child");
        drop(self.stdin.take());
        assert!(
            !status.success(),
            "forcibly terminated lifecycle probe unexpectedly succeeded"
        );
    }

    fn capture_remaining_output(&mut self) {
        loop {
            match self.output.recv_timeout(Duration::from_secs(1)) {
                Ok(Ok(line)) => self.captured_output.push_str(&line),
                Ok(Err(error)) => {
                    self.captured_output
                        .push_str(&format!("failed to read probe output: {error}\n"));
                    return;
                }
                Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {
                    return;
                }
            }
        }
    }
}

fn lifecycle_test_guard() -> MutexGuard<'static, ()> {
    LIFECYCLE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn spawn_probe_output_reader<R>(
    stream_name: &'static str,
    stream: R,
    output: mpsc::Sender<Result<String, String>>,
) where
    R: Read + Send + 'static,
{
    std::thread::Builder::new()
        .name(format!("runledger-container-lifecycle-{stream_name}"))
        .spawn(move || {
            for line in BufReader::new(stream).lines() {
                let line = line
                    .map(|line| format!("{line}\n"))
                    .map_err(|error| format!("{stream_name}: {error}"));
                let is_error = line.is_err();
                if output.send(line).is_err() || is_error {
                    break;
                }
            }
        })
        .expect("spawn lifecycle probe output reader");
}

fn external_postgres_is_configured() -> bool {
    let configured = std::env::var_os(TEST_ADMIN_DATABASE_URL_ENV).is_some();
    if configured {
        eprintln!(
            "skipping Docker-only lifecycle probe because {TEST_ADMIN_DATABASE_URL_ENV} is configured"
        );
    }
    configured
}

fn docker_lifecycle_assertions_are_unavailable() -> bool {
    if external_postgres_is_configured() {
        return true;
    }
    if docker_cli_is_available() {
        return false;
    }

    eprintln!(
        "skipping Docker-CLI lifecycle assertions because `{}` is unavailable or unresponsive",
        docker_cli().to_string_lossy()
    );
    true
}

fn docker_cli_is_available() -> bool {
    let mut child = match Command::new(docker_cli())
        .args(["version", "--format", "{{.Server.Version}}"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return false,
    };
    let deadline = Instant::now() + DOCKER_CLI_PROBE_TIMEOUT;

    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

impl Drop for ProbeProcess {
    fn drop(&mut self) {
        drop(self.stdin.take());
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        cleanup_owned_containers(&self.owner);
    }
}

fn unique_owner() -> String {
    let counter = PROBE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must follow Unix epoch")
        .as_nanos();
    format!("{}-{timestamp}-{counter}", std::process::id())
}

fn assert_container_removed(owner: &str, container_id: &str) {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;

    loop {
        let container_ids = owned_container_ids(owner);
        if container_ids.is_empty() {
            return;
        }

        if Instant::now() >= deadline {
            cleanup_owned_containers(owner);
            panic!(
                "container {container_id} was not removed within {CLEANUP_TIMEOUT:?}; remaining containers: {container_ids:?}"
            );
        }

        std::thread::sleep(CLEANUP_POLL_INTERVAL);
    }
}

fn owned_container_ids(owner: &str) -> Vec<String> {
    try_owned_container_ids(owner).expect("inspect lifecycle test containers")
}

fn try_owned_container_ids(owner: &str) -> Result<Vec<String>, String> {
    let filter = format!("label={PROCESS_OWNER_LABEL}={owner}");
    Ok(
        try_docker_output(["container", "ls", "--all", "--quiet", "--filter", &filter])?
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect(),
    )
}

fn cleanup_owned_containers(owner: &str) {
    let Ok(container_ids) = try_owned_container_ids(owner) else {
        return;
    };
    for container_id in container_ids {
        let _ = Command::new(docker_cli())
            .args(["container", "rm", "--force", "--volumes", &container_id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn docker_output<const N: usize>(args: [&str; N]) -> String {
    try_docker_output(args).expect("run Docker CLI for lifecycle test")
}

fn try_docker_output<const N: usize>(args: [&str; N]) -> Result<String, String> {
    let output = Command::new(docker_cli())
        .args(args)
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "Docker CLI failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout).map_err(|error| error.to_string())
}

fn docker_cli() -> std::ffi::OsString {
    std::env::var_os(DOCKER_CLI_ENV)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "docker".into())
}
