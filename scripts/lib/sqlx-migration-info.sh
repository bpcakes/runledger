#!/usr/bin/env bash

sqlx_migration_info_is_current() {
  awk '
    BEGIN {
      current = 1
      saw_migration = 0
    }

    /^[0-9]+\// {
      saw_migration = 1
      if ($0 !~ /^[0-9]+\/installed([[:space:]]|$)/ &&
          $0 !~ /^[0-9]+\/pending([[:space:]]|$)/) {
        current = 0
      }
    }

    /^[0-9]+\/pending([[:space:]]|$)/ {
      current = 0
    }

    /^(applied|local) migration (had|has) checksum[[:space:]]/ {
      current = 0
    }

    END {
      exit !(saw_migration && current)
    }
  '
}
