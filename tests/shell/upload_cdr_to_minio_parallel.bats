#!/usr/bin/env bats

setup() {
    REPOSITORY=$(cd "$BATS_TEST_DIRNAME/../.." && pwd)
    SCRIPT="$REPOSITORY/scripts/upload_cdr_to_minio_parallel.sh"
    CONFIG="$BATS_TEST_TMPDIR/config.json"
    TP="$BATS_TEST_TMPDIR/parquet"
    TEXT="$BATS_TEST_TMPDIR/text"
    export MOCK_MC_REMOTE="$BATS_TEST_TMPDIR/remote"
    export MOCK_MC_CALLS="$BATS_TEST_TMPDIR/calls.jsonl"
    export MC_CONFIG_DIR="$BATS_TEST_TMPDIR/mc-config"
    export TMPDIR="$BATS_TEST_TMPDIR/scratch"
    unset MC_UPLOAD_MULTIPART_THREADS
    mkdir -p "$TP/Semana110/vivo/volte" "$TEXT/Semana110/Vivo/FIXA" \
        "$TEXT/Semana110/Claro/home/u1/x/coleta/coletados/a_AXE" \
        "$MOCK_MC_REMOTE" "$MC_CONFIG_DIR/share" "$MC_CONFIG_DIR/certs/CAs" \
        "$BATS_TEST_TMPDIR/bin" "$TMPDIR"
    printf parquet > "$TP/Semana110/vivo/volte/a.parquet"
    printf text > "$TEXT/Semana110/Vivo/FIXA/b.gz"
    printf hidden > "$TEXT/Semana110/Vivo/FIXA/.c.gz"
    printf axe > "$TEXT/Semana110/Claro/home/u1/x/coleta/coletados/a_AXE/d.gz"
    printf '{"version":"10","aliases":{"minio":{"url":"https://minio.invalid"}}}\n' > "$MC_CONFIG_DIR/config.json"
    : > "$MOCK_MC_CALLS"
    cp "$BATS_TEST_DIRNAME/helpers/mock_mc_recursive.bash" "$BATS_TEST_TMPDIR/bin/mc"
    chmod +x "$BATS_TEST_TMPDIR/bin/mc"
    export PATH="$BATS_TEST_TMPDIR/bin:$PATH"
    jq -n --arg tp "$TP" --arg text "$TEXT" '{
        teleparser_folder:$tp, text_folder:$text,
        minio_config:{alias:"minio",destination:"cdr",batchsize:1},
        folder_mapping:{
            text:[
                {source_folder:"Vivo/FIXA",pattern:"*.gz",destination_folder:"vivo/stfc/fcdr"},
                {source_folder:"Claro/home/*/*/coleta/coletados/*_AXE",pattern:"*.gz",destination_folder:"claro/axe"}
            ],
            teleparser:[{source_folder:"vivo/volte",pattern:"*.parquet",destination_folder:"vivo/smp/volte"}]
        }
    }' > "$CONFIG"
}

upload() { bash "$SCRIPT" --config "$CONFIG" --week-dir Semana110 "$@"; }
assert_report() { printf '%s\n' "$output" | grep -Fx -- "$1: $2"; }
assert_object() { [[ -f "$MOCK_MC_REMOTE/minio/cdr/Semana110/$1" ]]; }
count_calls() { jq -s --arg c "$1" '[.[] | select(.command == $c)] | length' "$MOCK_MC_CALLS"; }

function uploads_both_categories_with_wildcards { # @test
    run upload
    [[ $status == 0 ]]
    assert_report status SUCCESS
    assert_report files_found 3
    assert_report files_transferred 3
    assert_report batches_launched 3
    assert_object vivo/stfc/fcdr/b.gz
    assert_object claro/axe/d.gz
    assert_object vivo/smp/volte/a.parquet
    [[ ! -e "$MOCK_MC_REMOTE/minio/cdr/Semana110/vivo/stfc/fcdr/.c.gz" ]]
}

function lists_remote_only_twice_regardless_of_batches { # @test
    run upload
    [[ $status == 0 ]]
    [[ $(count_calls cp) == 3 ]]
    [[ $(count_calls ls) == 2 ]]
    jq -es 'all(.[] | select(.command == "ls"); .args[3] == "minio/cdr/Semana110/")' "$MOCK_MC_CALLS"
}

function exports_multipart_threads_with_precedence { # @test
    run upload
    jq -es 'all(.[] | select(.command == "cp"); .threads == "16")' "$MOCK_MC_CALLS"
    : > "$MOCK_MC_CALLS"
    jq '.minio_config.multipart_threads = 8' "$CONFIG" > "$CONFIG.next" && mv "$CONFIG.next" "$CONFIG"
    run upload
    assert_report multipart_threads 8
    jq -es 'all(.[] | select(.command == "cp"); .threads == "8")' "$MOCK_MC_CALLS"
    : > "$MOCK_MC_CALLS"
    run upload --multipart-threads 32 --jobs 2
    assert_report multipart_threads 32
    assert_report jobs 2
    jq -es 'all(.[] | select(.command == "cp"); .threads == "32")' "$MOCK_MC_CALLS"
}

function runs_batches_in_parallel { # @test
    export MOCK_MC_SLEEP=1
    SECONDS=0
    run upload --jobs 3
    [[ $status == 0 ]]
    (( SECONDS < 3 ))
    assert_report files_transferred 3
}

function jobs_one_runs_sequentially { # @test
    export MOCK_MC_SLEEP=1
    SECONDS=0
    run upload --jobs 1
    [[ $status == 0 ]]
    (( SECONDS >= 3 ))
}

function dry_run_lists_but_does_not_copy { # @test
    run upload --dry-run
    [[ $status == 0 ]]
    assert_report status DRY_RUN
    assert_report files_planned 3
    [[ $(count_calls cp) == 0 ]]
    [[ $(count_calls ls) == 1 ]]
    printf '%s\n' "$output" | grep -F "PLAN $TEXT/Semana110/Vivo/FIXA/b.gz -> minio/cdr/Semana110/vivo/stfc/fcdr/b.gz"
}

function second_run_skips_existing_objects { # @test
    upload
    : > "$MOCK_MC_CALLS"
    run upload
    [[ $status == 0 ]]
    assert_report files_existing 3
    assert_report files_transferred 0
    [[ $(count_calls cp) == 0 ]]
}

function different_remote_size_is_never_overwritten { # @test
    upload
    printf XXXXXXXX > "$MOCK_MC_REMOTE/minio/cdr/Semana110/vivo/stfc/fcdr/b.gz"
    : > "$MOCK_MC_CALLS"
    run upload
    [[ $status == 1 ]]
    assert_report status PARTIAL_SUCCESS
    [[ $(count_calls cp) == 0 ]]
    [[ $(< "$MOCK_MC_REMOTE/minio/cdr/Semana110/vivo/stfc/fcdr/b.gz") == XXXXXXXX ]]
}

function failed_cp_is_reported_by_final_listing { # @test
    MOCK_MC_FAIL_NAME=b.gz run upload
    [[ $status == 1 ]]
    assert_report status PARTIAL_SUCCESS
    assert_report files_transferred 2
    [[ $output == *"Transfer not confirmed"* ]]
    ! assert_object vivo/stfc/fcdr/b.gz
    # Reexecução envia somente o que faltou.
    run upload
    [[ $status == 0 ]]
    assert_report files_existing 2
    assert_report files_transferred 1
}

function size_mismatch_after_upload_is_an_error { # @test
    MOCK_MC_WRONG_SIZE=a.parquet run upload
    [[ $status == 1 ]]
    [[ $output == *"Size mismatch after upload"* ]]
    assert_report files_transferred 2
}

function remote_listing_failure_is_fatal_and_uploads_nothing { # @test
    MOCK_MC_LS_FAIL=1 run upload
    [[ $status == 1 ]]
    assert_report status FAILED
    [[ $(count_calls cp) == 0 ]]
}

function only_text_folder_selects_text { # @test
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report categories text
    assert_report files_transferred 2
    ! assert_object vivo/smp/volte/a.parquet
}

function batches_group_by_count_and_destination { # @test
    printf more > "$TEXT/Semana110/Vivo/FIXA/e.gz"
    jq '.minio_config.batchsize = 2' "$CONFIG" > "$CONFIG.next" && mv "$CONFIG.next" "$CONFIG"
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report files_transferred 3
    assert_report batches_planned 2
    jq -es '[.[] | select(.command == "cp") | .args | length] | sort == [4, 5]' "$MOCK_MC_CALLS"
}

function symlink_is_reported_and_not_followed { # @test
    ln -s /etc/hostname "$TEXT/Semana110/Vivo/FIXA/link.gz"
    run upload
    [[ $status == 1 ]]
    [[ $output == *"Symbolic link not followed"* ]]
    ! assert_object vivo/stfc/fcdr/link.gz
}

function collision_aborts_before_upload { # @test
    mkdir -p "$TEXT/Semana110/Claro/home/u2/y/coleta/coletados/b_AXE"
    printf other > "$TEXT/Semana110/Claro/home/u2/y/coleta/coletados/b_AXE/d.gz"
    run upload
    [[ $status == 1 ]]
    [[ $output == *"Destination collision"* ]]
    [[ $(count_calls cp) == 0 ]]
}

function unmapped_teleparser_directory_blocks_category { # @test
    mkdir -p "$TP/Semana110/foo/bar"
    run upload
    [[ $status == 1 ]]
    assert_report pending_mappings 1
    ! assert_object vivo/smp/volte/a.parquet
    assert_object vivo/stfc/fcdr/b.gz
}

function rejects_invalid_arguments_and_config { # @test
    run upload --jobs 0
    [[ $status == 2 ]]
    run upload --multipart-threads abc
    [[ $status == 2 ]]
    run bash "$SCRIPT" --config "$CONFIG"
    [[ $status == 2 ]]
    jq '.minio_config.jobs = 0' "$CONFIG" > "$CONFIG.next" && mv "$CONFIG.next" "$CONFIG"
    run upload
    [[ $status == 2 ]]
    run bash "$SCRIPT" --help
    [[ $status == 0 ]]
}
