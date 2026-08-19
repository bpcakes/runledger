use std::ffi::OsString;
use std::io::{self, BufRead, BufReader};
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use testcontainers::{ContainerAsync, GenericImage};

pub(crate) const PROCESS_OWNER_LABEL: &str = "org.runledger.test-process";

const PROCESS_OWNER_ENV: &str = "RUNLEDGER_TEST_CONTAINER_OWNER";
const DOCKER_CLI_ENV: &str = "RUNLEDGER_TEST_DOCKER_CLI";
const TESTCONTAINERS_COMMAND_ENV: &str = "TESTCONTAINERS_COMMAND";
const REAPER_READY: &str = "runledger-container-reaper-ready";
const REAPER_READY_TIMEOUT: Duration = Duration::from_secs(15);
const REAPER_EXIT_GRACE_TIMEOUT: Duration = Duration::from_millis(250);

const REAPER_SCRIPT: &str = r#"
trap '' HUP INT QUIT TERM

if ! command -v "$1" >/dev/null 2>&1; then
    printf 'docker CLI `%s` is unavailable\n' "$1"
    exit 127
fi

if ! "$1" container inspect "$2" >/dev/null 2>&1; then
    printf 'docker CLI `%s` cannot inspect container %s\n' "$1" "$2"
    exit 1
fi

printf 'runledger-container-reaper-ready\n'

while IFS= read -r _; do
    :
done

"$1" container rm --force --volumes "$2" >/dev/null 2>&1 || :
"#;

pub(crate) struct ProcessContainer {
    // `SharedPostgres` is static and therefore is not dropped at process exit.
    // Keeping the handle here prevents early Testcontainers cleanup, while the
    // reaper's liveness pipe provides cleanup when the process disappears.
    container: ContainerAsync<GenericImage>,
    _reaper: Option<ContainerReaper>,
}

impl ProcessContainer {
    pub(crate) async fn new(container: ContainerAsync<GenericImage>) -> Self {
        let reaper = if should_remove_container() {
            let container_id = container.id().to_owned();
            match tokio::task::spawn_blocking(move || ContainerReaper::spawn(&container_id)).await {
                Ok(Ok(reaper)) => Some(reaper),
                Ok(Err(error)) => {
                    eprintln!(
                        "warning: process-liveness container cleanup is unavailable: {error}"
                    );
                    None
                }
                Err(error) => {
                    eprintln!("warning: process-liveness container cleanup task failed: {error}");
                    None
                }
            }
        } else {
            None
        };

        Self {
            container,
            _reaper: reaper,
        }
    }

    pub(crate) fn container(&self) -> &ContainerAsync<GenericImage> {
        &self.container
    }
}

pub(crate) fn process_owner_label_value() -> String {
    std::env::var(PROCESS_OWNER_ENV)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| std::process::id().to_string())
}

fn should_remove_container() -> bool {
    std::env::var(TESTCONTAINERS_COMMAND_ENV).as_deref() != Ok("keep")
}

struct ContainerReaper {
    child: Child,
    // The reaper blocks on this pipe. Normal exit, abort, and forced process
    // termination all close the OS handle and release the reaper.
    stdin: Option<ChildStdin>,
}

impl ContainerReaper {
    fn spawn(container_id: &str) -> io::Result<Self> {
        let docker_cli = std::env::var_os(DOCKER_CLI_ENV)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| OsString::from("docker"));
        let mut command = reaper_command(&docker_cli, container_id);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("container reaper stdin was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("container reaper stdout was not piped"))?;
        let ready = match read_reaper_ready(stdout) {
            Ok(ready) => ready,
            Err(error) => {
                let _ = terminate_reaper(&mut child);
                return Err(error);
            }
        };

        if ready.trim() != REAPER_READY {
            drop(stdin);
            let detail = ready.trim();
            let status = terminate_reaper(&mut child).map_err(|termination_error| {
                io::Error::other(format!(
                    "container reaper failed to start: {detail}; cleanup wait also failed: {termination_error}"
                ))
            })?;
            return Err(io::Error::other(format!(
                "container reaper failed to start ({status}): {detail}"
            )));
        }

        Ok(Self {
            child,
            stdin: Some(stdin),
        })
    }
}

fn terminate_reaper(child: &mut Child) -> io::Result<std::process::ExitStatus> {
    let deadline = Instant::now() + REAPER_EXIT_GRACE_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    terminate_reaper_process_tree(child);
    let _ = child.kill();
    child.wait()
}

fn terminate_reaper_process_tree(child: &Child) {
    let process_group = format!("-{}", child.id());
    let _ = Command::new("kill")
        .args(["-KILL", "--", &process_group])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn read_reaper_ready(stdout: std::process::ChildStdout) -> io::Result<String> {
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("runledger-container-reaper-ready".to_owned())
        .spawn(move || {
            let mut ready = String::new();
            let result = BufReader::new(stdout).read_line(&mut ready).map(|_| ready);
            let _ = sender.send(result);
        })?;

    match receiver.recv_timeout(REAPER_READY_TIMEOUT) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("container reaper did not become ready within {REAPER_READY_TIMEOUT:?}"),
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(io::Error::other(
            "container reaper readiness reader stopped unexpectedly",
        )),
    }
}

impl Drop for ContainerReaper {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.wait();
    }
}

fn reaper_command(docker_cli: &OsString, container_id: &str) -> Command {
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(REAPER_SCRIPT)
        .arg("runledger-container-reaper")
        .arg(docker_cli)
        .arg(container_id)
        .process_group(0);
    command
}
