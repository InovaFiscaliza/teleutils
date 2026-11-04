#!/usr/bin/env bats

setup() {
    REPOSITORY=$(cd "$BATS_TEST_DIRNAME/../.." && pwd)
    SCRIPT="$REPOSITORY/scripts/copy_cdr_to_local.sh"
    CONFIG="$BATS_TEST_TMPDIR/config.json"
    TP="$BATS_TEST_TMPDIR/parquet"
    TEXT="$BATS_TEST_TMPDIR/text"
    OUT="$BATS_TEST_TMPDIR/out"
    export TMPDIR="$BATS_TEST_TMPDIR/scratch"
    mkdir -p "$TP/Semana110/vivo/volte" "$TEXT/Semana110/Vivo/FIXA" \
        "$TEXT/Semana110/Claro/home/u1/x/coleta/coletados/a_AXE" "$TMPDIR"
    printf parquet > "$TP/Semana110/vivo/volte/a.parquet"
    printf text > "$TEXT/Semana110/Vivo/FIXA/b.gz"
    printf hidden > "$TEXT/Semana110/Vivo/FIXA/.c.gz"
    printf axe > "$TEXT/Semana110/Claro/home/u1/x/coleta/coletados/a_AXE/d.gz"
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

copy() { bash "$SCRIPT" --config "$CONFIG" --week-dir Semana110 --destination "$OUT" "$@"; }
assert_report() { printf '%s\n' "$output" | grep -Fx -- "$1: $2"; }

function copies_both_categories_with_wildcards { # @test
    run copy --jobs 2
    [[ $status == 0 ]]
    assert_report status SUCCESS
    assert_report files_transferred 3
    assert_report batches_planned 3
    [[ $(< "$OUT/Semana110/vivo/stfc/fcdr/b.gz") == text ]]
    [[ $(< "$OUT/Semana110/claro/axe/d.gz") == axe ]]
    [[ $(< "$OUT/Semana110/vivo/smp/volte/a.parquet") == parquet ]]
    [[ ! -e "$OUT/Semana110/vivo/stfc/fcdr/.c.gz" ]]
    [[ -z $(find "$OUT" -name '.partial*') ]]
}

function dry_run_writes_nothing { # @test
    run copy --dry-run
    [[ $status == 0 ]]
    assert_report status DRY_RUN
    assert_report files_planned 3
    [[ ! -e $OUT ]]
    printf '%s\n' "$output" | grep -F "PLAN $TEXT/Semana110/Vivo/FIXA/b.gz -> $OUT/Semana110/vivo/stfc/fcdr/b.gz"
}

function second_run_is_idempotent { # @test
    copy
    run copy
    [[ $status == 0 ]]
    assert_report files_existing 3
    assert_report files_transferred 0
    assert_report batches_planned 0
}

function different_size_is_never_overwritten { # @test
    copy
    printf XXXXXXXX > "$OUT/Semana110/vivo/stfc/fcdr/b.gz"
    run copy
    [[ $status == 1 ]]
    assert_report status PARTIAL_SUCCESS
    [[ $(< "$OUT/Semana110/vivo/stfc/fcdr/b.gz") == XXXXXXXX ]]
}

function only_text_folder_selects_text { # @test
    run copy --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report categories text
    assert_report files_transferred 2
    [[ ! -e "$OUT/Semana110/vivo/smp" ]]
}

function destination_from_local_config { # @test
    edit="$BATS_TEST_TMPDIR/config2.json"
    jq --arg d "$OUT" '.local_config={destination:$d,jobs:1}' "$CONFIG" > "$edit"
    run bash "$SCRIPT" --config "$edit" --week-dir Semana110
    [[ $status == 0 ]]
    assert_report jobs 1
    [[ -f "$OUT/Semana110/vivo/stfc/fcdr/b.gz" ]]
}

function symlink_is_reported_and_not_followed { # @test
    ln -s /etc/hostname "$TEXT/Semana110/Vivo/FIXA/link.gz"
    run copy
    [[ $status == 1 ]]
    [[ ! -e "$OUT/Semana110/vivo/stfc/fcdr/link.gz" ]]
    [[ $output == *"Symbolic link not followed"* ]]
}

function collision_aborts_before_copy { # @test
    mkdir -p "$TEXT/Semana110/Claro/home/u2/y/coleta/coletados/b_AXE"
    printf other > "$TEXT/Semana110/Claro/home/u2/y/coleta/coletados/b_AXE/d.gz"
    run copy
    [[ $status == 1 ]]
    [[ $output == *"Destination collision"* ]]
    [[ ! -e $OUT ]]
}

function unmapped_teleparser_directory_blocks_category { # @test
    mkdir -p "$TP/Semana110/foo/bar"
    run copy
    [[ $status == 1 ]]
    assert_report pending_mappings 1
    [[ ! -e "$OUT/Semana110/vivo/smp" ]]
    [[ -f "$OUT/Semana110/vivo/stfc/fcdr/b.gz" ]]
}

function rejects_destination_overlapping_source { # @test
    run bash "$SCRIPT" --config "$CONFIG" --week-dir Semana110 --destination "$TEXT"
    [[ $status == 2 ]]
    [[ $output == *"overlaps"* ]]
}

function rejects_invalid_arguments { # @test
    run bash "$SCRIPT" --config "$CONFIG" --week-dir Semana110 --destination "$OUT" --jobs 0
    [[ $status == 2 ]]
    run bash "$SCRIPT" --config "$CONFIG" --week-dir Semana110
    [[ $status == 2 ]]
    run bash "$SCRIPT" --help
    [[ $status == 0 ]]
}
