#!/usr/bin/env bats

# A sintaxe de funções com @test permite também validar o arquivo com bash -n.
setup() {
    REPOSITORY=$(cd "$BATS_TEST_DIRNAME/../.." && pwd)
    SCRIPT="$REPOSITORY/scripts/upload_cdr_to_minio.sh"
    CONFIG="$BATS_TEST_TMPDIR/config.json"
    TP="$BATS_TEST_TMPDIR/parquet"
    TEXT="$BATS_TEST_TMPDIR/text"
    export MOCK_MC_REMOTE="$BATS_TEST_TMPDIR/remote"
    export MOCK_MC_CALLS="$BATS_TEST_TMPDIR/calls.jsonl"
    export MC_CONFIG_DIR="$BATS_TEST_TMPDIR/mc-config"
    export TMPDIR="$BATS_TEST_TMPDIR/scratch"
    mkdir -p "$TP/Semana110/vivo/volte" "$TEXT/Semana110/Vivo/FIXA" \
        "$MOCK_MC_REMOTE" "$MC_CONFIG_DIR/share" "$MC_CONFIG_DIR/certs/CAs" \
        "$BATS_TEST_TMPDIR/bin" "$TMPDIR"
    printf parquet > "$TP/Semana110/vivo/volte/a.parquet"
    printf text > "$TEXT/Semana110/Vivo/FIXA/b.gz"
    printf '{"version":"10","aliases":{"minio":{"url":"https://minio.invalid"}}}\n' > "$MC_CONFIG_DIR/config.json"
    : > "$MOCK_MC_CALLS"
    cp "$BATS_TEST_DIRNAME/helpers/mock_mc.bash" "$BATS_TEST_TMPDIR/bin/mc"
    chmod +x "$BATS_TEST_TMPDIR/bin/mc"
    export PATH="$BATS_TEST_TMPDIR/bin:$PATH"
    jq -n --arg tp "$TP" --arg text "$TEXT" '{
        teleparser_folder:$tp, text_folder:$text,
        minio_config:{alias:"minio",destination:"cdr",batchsize:2},
        folder_mapping:{
            text:[{source_folder:"Vivo/FIXA",pattern:"*.gz",destination_folder:"vivo/stfc/fcdr"}],
            teleparser:[{source_folder:"vivo/volte",pattern:"*.parquet",destination_folder:"vivo/smp/volte"}]
        }
    }' > "$CONFIG"
}

upload() { bash "$SCRIPT" --config "$CONFIG" --week-dir Semana110 "$@"; }
edit_config() {
    jq "$@" "$CONFIG" > "$CONFIG.next" && mv "$CONFIG.next" "$CONFIG"
}
assert_report() { printf '%s\n' "$output" | grep -Fx -- "$1: $2"; }
assert_no_copy() { jq -es 'all(.[]; .command != "cp")' "$MOCK_MC_CALLS"; }
assert_object() { [[ -f "$MOCK_MC_REMOTE/minio/cdr/Semana110/$1" ]]; }
put_remote() {
    local target="$MOCK_MC_REMOTE/minio/cdr/Semana110/$2"
    mkdir -p -- "${target%/*}"
    cp -- "$1" "$target"
}

function default_processes_both_categories { # @test
    run upload
    [[ $status == 0 ]]
    assert_report status SUCCESS
    assert_report files_found 2
    assert_report files_selected 2
    assert_report files_transferred 2
    assert_report batches_processed 2
    assert_object vivo/stfc/fcdr/b.gz
    assert_object vivo/smp/volte/a.parquet
    jq -es '[.[] | select(.command=="cp") | .args[-1]] ==
        ["minio/cdr/Semana110/vivo/stfc/fcdr/", "minio/cdr/Semana110/vivo/smp/volte/"]' "$MOCK_MC_CALLS"
}

function explicit_teleparser_ignores_invalid_text_configuration { # @test
    edit_config '.text_folder = null | .folder_mapping.text = "invalid"'
    run upload --teleparser-folder "$TP"
    [[ $status == 0 ]]
    assert_report categories teleparser
    assert_report files_transferred 1
    [[ $output != *source_text:* ]]
    [[ ! -e "$MOCK_MC_REMOTE/minio/cdr/Semana110/vivo/stfc" ]]
}

function explicit_text_ignores_missing_teleparser_configuration { # @test
    edit_config 'del(.teleparser_folder, .folder_mapping.teleparser)'
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report categories text
    assert_report files_transferred 1
    [[ $output != *source_teleparser:* ]]
}

function both_overrides_replace_both_json_roots { # @test
    edit_config '.teleparser_folder="/missing" | .text_folder=null'
    run upload --teleparser-folder "$TP" --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report files_transferred 2
}

function mandatory_week_and_config_and_argument_validation { # @test
    local args
    for args in config week unknown config-file missing-value repeated; do
        case "$args" in
            config) run bash "$SCRIPT" --week-dir Semana110 ;;
            week) run bash "$SCRIPT" --config "$CONFIG" ;;
            unknown) run upload --unexpected ;;
            config-file) run bash "$SCRIPT" --config-file "$CONFIG" --week-dir Semana200 --teleparser-folder "$TP" ;;
            missing-value) run upload --text-folder ;;
            repeated) run upload --destination cdr --destination other ;;
        esac
        [[ $status == 2 ]]
        [[ $output == "[ERROR]"*$'\nUsage:'* ]]
        [[ $output != *"FINAL REPORT"* ]]
        [[ $output != *"UNRESOLVED"* ]]
    done
    [[ ! -s $MOCK_MC_CALLS ]]
}

function no_arguments_shows_error_and_usage_without_report_or_tools { # @test
    run env PATH=/nonexistent /bin/bash "$SCRIPT"
    [[ $status == 2 ]]
    [[ $output == "[ERROR] Both --config and --week-dir are required."$'\nUsage:'* ]]
    [[ $output != *"FINAL REPORT"* ]]
    [[ $output != *"UNRESOLVED"* ]]
    [[ ! -s $MOCK_MC_CALLS ]]
}

function help_does_not_require_configuration_or_tools { # @test
    run env PATH=/nonexistent /bin/bash "$SCRIPT" --help
    [[ $status == 0 ]]
    [[ $output == *"process ONLY Teleparser"* ]]
    [[ $output == *"BASE directories"* ]]
    [[ $output != *"FINAL REPORT"* ]]
}

function invalid_weeks_are_rejected { # @test
    local week
    for week in ../Semana110 Semana110/../Semana111 /Semana110 Semana110/ x Semana; do
        run bash "$SCRIPT" --config "$CONFIG" --week-dir "$week"
        [[ $status == 2 ]]
        [[ $output == *"Invalid week name:"* ]]
        [[ $output == "[ERROR]"*$'\nUsage:'* ]]
        [[ $output != *"FINAL REPORT"* ]]
    done
    assert_no_copy
}

function sources_use_only_the_requested_week { # @test
    mkdir -p "$TP/vivo/volte" "$TP/Semana111/vivo/volte"
    printf wrong > "$TP/vivo/volte/base.parquet"
    printf wrong > "$TP/Semana111/vivo/volte/other.parquet"
    run upload --teleparser-folder "$TP"
    [[ $status == 0 ]]
    assert_report files_found 1
    assert_report files_transferred 1
}

function missing_selected_week_blocks_all_copies { # @test
    run upload --text-folder "$TEXT" --teleparser-folder "$BATS_TEST_TMPDIR/missing"
    [[ $status == 2 ]]
    assert_report files_transferred 0
    assert_no_copy
}

function weekly_directory_cannot_be_used_as_base { # @test
    run upload --teleparser-folder "$TP/Semana110"
    [[ $status == 2 ]]
    assert_no_copy
}

function invalid_json_and_multiple_json_documents_are_rejected { # @test
    printf '{invalid' > "$CONFIG"
    run upload
    [[ $status == 2 ]]
    printf '{}\n{}\n' > "$CONFIG"
    run upload
    [[ $status == 2 ]]
    assert_no_copy
}

function invalid_batch_sizes_are_rejected { # @test
    local size
    for size in 0 -1 1.5 '"2"' null 2147483648; do
        edit_config ".minio_config.batchsize=$size"
        run upload
        [[ $status == 2 ]]
    done
    assert_no_copy
}

function nul_configuration_fields_are_rejected { # @test
    local initial field
    initial=$(jq -c . "$CONFIG")
    for field in minio_config.alias minio_config.destination text_folder teleparser_folder \
        folder_mapping.text.0.source_folder folder_mapping.text.0.pattern \
        folder_mapping.text.0.destination_folder folder_mapping.teleparser.0.source_folder \
        folder_mapping.teleparser.0.pattern folder_mapping.teleparser.0.destination_folder; do
        edit_config --arg field "$field" --argjson initial "$initial" '
            $initial | setpath(($field | split(".") | map(tonumber? // .)); "bad\u0000value")'
        run upload
        [[ $status == 2 ]]
        assert_report status FAILED
    done
    [[ ! -s $MOCK_MC_CALLS ]]
}

function invalid_mapping_fields_and_traversal_are_rejected { # @test
    local value
    for value in '"../escape"' '"/absolute"' '"vivo/../escape"' '""' null '"bad\u0000name"'; do
        edit_config ".folder_mapping.teleparser[0].source_folder=$value"
        run upload
        [[ $status == 2 ]]
    done
    assert_no_copy
}

function week_cannot_be_duplicated_in_target_components { # @test
    run upload --destination cdr/Semana110
    [[ $status == 2 ]]
    edit_config '.folder_mapping.teleparser[0].destination_folder="vivo/Semana110/volte"'
    run upload
    [[ $status == 2 ]]
    assert_no_copy
}

function destination_override_and_separator_normalization { # @test
    edit_config '.minio_config.destination=null |
        .folder_mapping.teleparser[0].source_folder="vivo//volte/" |
        .folder_mapping.teleparser[0].destination_folder="vivo//smp/volte/"'
    run upload --teleparser-folder "$TP//" --destination backup//prefix/
    [[ $status == 0 ]]
    [[ -f "$MOCK_MC_REMOTE/minio/backup/prefix/Semana110/vivo/smp/volte/a.parquet" ]]
    jq -es '[.[] | select(.command=="cp") | .args[-1]] ==
        ["minio/backup/prefix/Semana110/vivo/smp/volte/"]' "$MOCK_MC_CALLS"
}

function legacy_environment_settings_do_not_override_json { # @test
    export MINIO_DESTINATION=unexpected/wrong BATCH_SIZE=0
    run upload
    [[ $status == 0 ]]
    assert_report destination_base minio/cdr
    assert_report files_transferred 2
}

function unmapped_teleparser_blocks_only_teleparser { # @test
    mkdir -p "$TP/Semana110/unknown/vendor"
    run upload
    [[ $status == 1 ]]
    assert_report status PARTIAL_SUCCESS
    assert_report files_transferred 1
    assert_object vivo/stfc/fcdr/b.gz
    [[ ! -e "$MOCK_MC_REMOTE/minio/cdr/Semana110/vivo/smp" ]]
}

function unmapped_text_is_ignored { # @test
    mkdir -p "$TEXT/Semana110/Unknown/FIXA"
    printf data > "$TEXT/Semana110/Unknown/FIXA/unmapped.gz"
    run upload
    [[ $status == 0 ]]
    assert_report files_found 2
}

function absent_and_empty_mappings_remain_pending { # @test
    edit_config '.folder_mapping.text += [
        {source_folder:"Absent/FIXA",pattern:"*.gz",destination_folder:"absent"},
        {source_folder:"Empty/FIXA",pattern:"*.gz",destination_folder:"empty"}]'
    mkdir -p "$TEXT/Semana110/Empty/FIXA"
    run upload
    [[ $status == 1 ]]
    assert_report pending_mappings 2
    assert_report files_transferred 2
    assert_report status PARTIAL_SUCCESS
}

function glob_directories_are_flattened_and_not_recursive { # @test
    edit_config '.folder_mapping.text = [
        {source_folder:"Claro/home/*/*/coleta/coletados/*_AXE",pattern:"*.gz",destination_folder:"claro/stfc/axe"}]'
    local path="$TEXT/Semana110/Claro/home/a/b/coleta/coletados/switch_AXE"
    mkdir -p "$path/nested"
    printf direct > "$path/axe.gz"
    printf nested > "$path/nested/ignored.gz"
    printf hidden > "$path/.ignored.gz"
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report files_found 1
    assert_object claro/stfc/axe/axe.gz
}

function mappings_may_point_directly_to_files { # @test
    edit_config '.folder_mapping.text[0].source_folder="Vivo/FIXA/*.gz"'
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report files_transferred 1
}

function special_filenames_roundtrip_through_mc_json { # @test
    local name
    local -a names=("space name.gz" 'quote"and\slash.gz' $'tab\tline\nend\n.gz'
        '$(touch SHOULD_NOT_EXIST).gz' '-leading.gz' '[literal]*?.gz')
    for name in "${names[@]}"; do printf special > "$TEXT/Semana110/Vivo/FIXA/$name"; done
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report files_transferred 7
    for name in "${names[@]}"; do
        assert_object "vivo/stfc/fcdr/$name"
        cmp -- "$TEXT/Semana110/Vivo/FIXA/$name" "$MOCK_MC_REMOTE/minio/cdr/Semana110/vivo/stfc/fcdr/$name"
    done
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report files_existing 7
    assert_report files_transferred 0
    [[ ! -e SHOULD_NOT_EXIST ]]
}

function base_paths_with_glob_characters_are_literal { # @test
    local base="$BATS_TEST_TMPDIR/literal[1]*"$'\n'
    mkdir -p "$base"
    cp -R "$TEXT/Semana110" "$base/"
    run upload --text-folder "$base"
    [[ $status == 0 ]]
    assert_report files_transferred 1
}

function duplicate_mappings_are_deduplicated { # @test
    edit_config '.folder_mapping.text += .folder_mapping.text'
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report files_found 1
    assert_report files_selected 1
    assert_report files_transferred 1
}

function duplicate_targets_from_distinct_same_size_files_abort_globally { # @test
    mkdir -p "$TEXT/Semana110/Other/FIXA"
    cp "$TEXT/Semana110/Vivo/FIXA/b.gz" "$TEXT/Semana110/Other/FIXA/b.gz"
    edit_config '.folder_mapping.text += [
        {source_folder:"Other/FIXA",pattern:"*.gz",destination_folder:"vivo/stfc/fcdr"}]'
    run upload
    [[ $status == 1 ]]
    assert_report status FAILED
    assert_report files_transferred 0
    assert_no_copy
}

function cross_category_target_collisions_abort_globally { # @test
    cp "$TP/Semana110/vivo/volte/a.parquet" "$TEXT/Semana110/Vivo/FIXA/a.parquet"
    edit_config '.folder_mapping.text[0].pattern="*.parquet" |
        .folder_mapping.text[0].destination_folder="vivo/smp/volte"'
    run upload
    [[ $status == 1 ]]
    assert_no_copy
}

function internal_file_and_directory_links_are_supported { # @test
    ln -s a.parquet "$TP/Semana110/vivo/volte/link.parquet"
    ln -s Vivo "$TEXT/Semana110/Linked"
    edit_config '.folder_mapping.text[0].source_folder="Linked/FIXA"'
    run upload
    [[ $status == 0 ]]
    assert_report files_transferred 3
    assert_object vivo/smp/volte/link.parquet
}

function external_and_broken_links_are_not_transferred { # @test
    printf outside > "$BATS_TEST_TMPDIR/outside.gz"
    ln -s "$BATS_TEST_TMPDIR/outside.gz" "$TEXT/Semana110/Vivo/FIXA/external.gz"
    ln -s nonexistent.gz "$TEXT/Semana110/Vivo/FIXA/broken.gz"
    run upload --text-folder "$TEXT"
    [[ $status == 1 ]]
    assert_report status PARTIAL_SUCCESS
    assert_report files_transferred 1
    [[ ! -e "$MOCK_MC_REMOTE/minio/cdr/Semana110/vivo/stfc/fcdr/external.gz" ]]
}

function external_directory_links_are_not_traversed { # @test
    mkdir -p "$BATS_TEST_TMPDIR/outside"
    printf outside > "$BATS_TEST_TMPDIR/outside/outside.gz"
    ln -s "$BATS_TEST_TMPDIR/outside" "$TEXT/Semana110/External"
    edit_config '.folder_mapping.text[0].source_folder="External"'
    run upload --text-folder "$TEXT"
    [[ $status == 1 ]]
    assert_report files_transferred 0
    assert_no_copy
}

function identical_remote_objects_are_skipped { # @test
    put_remote "$TEXT/Semana110/Vivo/FIXA/b.gz" vivo/stfc/fcdr/b.gz
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report status SUCCESS
    assert_report files_existing 1
    assert_report files_transferred 0
    assert_report batches_processed 0
    assert_no_copy
}

function remote_size_conflicts_are_not_overwritten { # @test
    put_remote "$TEXT/Semana110/Vivo/FIXA/b.gz" vivo/stfc/fcdr/b.gz
    printf different >> "$MOCK_MC_REMOTE/minio/cdr/Semana110/vivo/stfc/fcdr/b.gz"
    run upload
    [[ $status == 1 ]]
    assert_report status PARTIAL_SUCCESS
    assert_report files_transferred 1
    [[ $(cat "$MOCK_MC_REMOTE/minio/cdr/Semana110/vivo/stfc/fcdr/b.gz") == textdifferent ]]
}

function batch_size_is_a_per_destination_maximum { # @test
    printf c > "$TEXT/Semana110/Vivo/FIXA/c.gz"
    printf d > "$TEXT/Semana110/Vivo/FIXA/d.gz"
    printf e > "$TEXT/Semana110/Vivo/FIXA/e.gz"
    printf f > "$TEXT/Semana110/Vivo/FIXA/f.gz"
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report files_transferred 5
    assert_report batches_processed 3
    jq -es '[.[] | select(.command=="cp") | (.args | length)-3] == [2,2,1]' "$MOCK_MC_CALLS"
}

function batch_size_one_is_supported { # @test
    edit_config '.minio_config.batchsize=1'
    printf c > "$TEXT/Semana110/Vivo/FIXA/c.gz"
    run upload --text-folder "$TEXT"
    [[ $status == 0 ]]
    assert_report batches_processed 2
}

function dry_run_does_not_write_or_count_simulated_transfers { # @test
    local before after
    before=$(find "$TP" "$TEXT" "$MOCK_MC_REMOTE" "$MC_CONFIG_DIR" "$TMPDIR" \
        -type f -exec sha256sum -- {} + | sort)
    run upload --dry-run
    [[ $status == 0 ]]
    assert_report status DRY_RUN
    assert_report files_planned 2
    assert_report files_transferred 0
    assert_report batches_planned 2
    assert_report batches_processed 0
    [[ $output == *"PLAN "* ]]
    assert_no_copy
    jq -es 'all(.[]; .command=="--version" or .command=="stat" or .command=="ls")' "$MOCK_MC_CALLS"
    after=$(find "$TP" "$TEXT" "$MOCK_MC_REMOTE" "$MC_CONFIG_DIR" "$TMPDIR" \
        -type f -exec sha256sum -- {} + | sort)
    [[ $before == "$after" ]]
    [[ -z $(find "$TMPDIR" -mindepth 1 -print) ]]
}

function dry_run_reports_existing_objects_and_pending_mappings { # @test
    put_remote "$TEXT/Semana110/Vivo/FIXA/b.gz" vivo/stfc/fcdr/b.gz
    edit_config '.folder_mapping.text += [
        {source_folder:"Absent",pattern:"*.gz",destination_folder:"absent"}]'
    run upload --dry-run
    [[ $status == 1 ]]
    assert_report status DRY_RUN
    assert_report files_existing 1
    assert_report files_planned 1
    assert_report files_transferred 0
    assert_no_copy
}

function partial_copy_failure_is_verified_and_next_batches_continue { # @test
    export MOCK_MC_FAIL_NAME=b.gz
    printf c > "$TEXT/Semana110/Vivo/FIXA/c.gz"
    printf d > "$TEXT/Semana110/Vivo/FIXA/d.gz"
    run upload
    [[ $status == 1 ]]
    assert_report status PARTIAL_SUCCESS
    assert_report files_transferred 3
    assert_report batches_processed 3
    assert_object vivo/stfc/fcdr/c.gz
    assert_object vivo/stfc/fcdr/d.gz
    assert_object vivo/smp/volte/a.parquet
}

function success_exit_from_cp_alone_is_not_confirmation { # @test
    export MOCK_MC_LIE_NAME=b.gz
    run upload --text-folder "$TEXT"
    [[ $status == 1 ]]
    assert_report status FAILED
    assert_report files_transferred 0
}

function wrong_uploaded_size_is_not_confirmation { # @test
    export MOCK_MC_WRONG_SIZE=b.gz
    run upload --text-folder "$TEXT"
    [[ $status == 1 ]]
    assert_report files_transferred 0
}

function changing_local_source_is_not_certified { # @test
    export MOCK_MC_MUTATE_SOURCE=b.gz
    run upload --text-folder "$TEXT"
    [[ $status == 1 ]]
    assert_report files_transferred 0
}

function failed_remote_listing_is_not_treated_as_empty { # @test
    export MOCK_MC_FAIL_LIST=minio/cdr/Semana110/vivo/stfc/fcdr
    run upload
    [[ $status == 1 ]]
    assert_report status PARTIAL_SUCCESS
    assert_report files_transferred 1
    jq -es 'all(.[] | select(.command=="cp"); .args[-1] !=
        "minio/cdr/Semana110/vivo/stfc/fcdr/")' "$MOCK_MC_CALLS"
}

function invalid_or_error_json_does_not_authorize_copy { # @test
    export MOCK_MC_BAD_JSON=1
    run upload
    [[ $status == 1 ]]
    assert_report files_transferred 0
    assert_no_copy
    unset MOCK_MC_BAD_JSON
    export MOCK_MC_ERROR_JSON=1
    run upload
    [[ $status == 1 ]]
    assert_no_copy
}

function nul_remote_keys_do_not_authorize_copy { # @test
    export MOCK_MC_NUL_KEY=1
    run upload
    [[ $status == 1 ]]
    assert_report status FAILED
    assert_report files_transferred 0
    assert_no_copy
}

function failed_post_copy_listing_prevents_success_count { # @test
    export MOCK_MC_VERIFY_FAIL=1
    run upload --text-folder "$TEXT"
    [[ $status == 1 ]]
    assert_report files_transferred 0
    assert_report status FAILED
}

function bucket_access_failure_is_an_environment_error { # @test
    export MOCK_MC_STAT_FAIL=1
    run upload
    [[ $status == 2 ]]
    assert_report status FAILED
    assert_no_copy
}

function missing_alias_config_is_not_created { # @test
    export MC_CONFIG_DIR="$BATS_TEST_TMPDIR/not-created"
    run upload --dry-run
    [[ $status == 2 ]]
    [[ ! -e $MC_CONFIG_DIR ]]
    assert_no_copy
}

function legacy_mc_state_is_rejected_before_any_client_invocation { # @test
    printf '{}\n' > "$MC_CONFIG_DIR/share/urls.json"
    run upload --dry-run
    [[ $status == 2 ]]
    [[ ! -s $MOCK_MC_CALLS ]]
    [[ -f "$MC_CONFIG_DIR/share/urls.json" ]]
}

function discovery_errors_are_not_treated_as_empty_success { # @test
    printf '#!/usr/bin/env bash\nexit 1\n' > "$BATS_TEST_TMPDIR/bin/find"
    chmod +x "$BATS_TEST_TMPDIR/bin/find"
    run upload --text-folder "$TEXT"
    [[ $status == 1 ]]
    assert_report status FAILED
    assert_no_copy
}

function missing_dependency_is_reported { # @test
    local command_name
    mkdir -p "$BATS_TEST_TMPDIR/limited-path"
    for command_name in jq find sort realpath stat; do
        ln -s "$(command -v "$command_name")" "$BATS_TEST_TMPDIR/limited-path/$command_name"
    done
    run env PATH="$BATS_TEST_TMPDIR/limited-path" /bin/bash "$SCRIPT" --config "$CONFIG" --week-dir Semana110
    [[ $status == 2 ]]
    [[ $output == *"Required command not found: mc."* ]]
}

function sources_are_unchanged_after_success { # @test
    local before
    before=$(find "$TP" "$TEXT" -type f -exec sha256sum -- {} + | sort)
    run upload
    [[ $status == 0 ]]
    [[ $(find "$TP" "$TEXT" -type f -exec sha256sum -- {} + | sort) == "$before" ]]
    [[ $output =~ duration_seconds:\ [0-9]+ ]]
}

function all_23_original_mappings_are_exercised { # @test
    jq --arg tp "$TP" --arg text "$TEXT" '
        .teleparser_folder=$tp | .text_folder=$text |
        .minio_config.alias="minio" | .minio_config.destination="cdr"
    ' "$REPOSITORY/scripts/config.example.json" > "$CONFIG"
    local category source pattern destination base name
    while IFS= read -r -d '' category &&
          IFS= read -r -d '' source &&
          IFS= read -r -d '' pattern &&
          IFS= read -r -d '' destination; do
        if [[ $category == text ]]; then base=$TEXT; name=b.gz; else base=$TP; name=a.parquet; fi
        [[ $pattern != '*TN*.gz' ]] || name=sampleTN.gz
        [[ $pattern != '*7N*.gz' ]] || name=sample7N.gz
        source=${source//\*/segment}
        mkdir -p "$base/Semana110/$source"
        printf fixture > "$base/Semana110/$source/$name"
    done < <(jq -j '.folder_mapping | to_entries[] |
        .key as $category | .value[] |
        $category, "\u0000", .source_folder, "\u0000",
        .pattern, "\u0000", .destination_folder, "\u0000"' "$CONFIG")
    run upload
    [[ $status == 0 ]]
    assert_report files_found 23
    assert_report files_transferred 23
    assert_report pending_mappings 0
}

function example_mapping_values_match_the_original_scripts { # @test
    local category script entry source pattern destination actual expected=""
    for category in text teleparser; do
        script="$REPOSITORY/tests/scripts/upload_${category}_cdr_to_minio.sh"
        expected=""
        while IFS= read -r entry; do
            [[ $entry == *'"'* ]] || continue
            entry=${entry#*\"}
            entry=${entry%\"*}
            IFS='|' read -r source pattern destination <<< "$entry"
            expected+=$(jq -cn --arg source "$source" --arg pattern "$pattern" --arg destination "$destination" \
                'def trim: gsub("^\\s+|\\s+$";"");
                {source_folder:($source|trim|rtrimstr("/")),pattern:($pattern|trim),
                destination_folder:($destination|trim)}')
            expected+=$'\n'
        done < <(sed -n '/^FOLDER_MAPPING=(/,/^)/p' "$script")
        actual=$(jq -c --arg category "$category" '.folder_mapping[$category][]' \
            "$REPOSITORY/scripts/config.example.json")
        [[ $(jq -csS . <<< "$expected") == "$(jq -csS . <<< "$actual")" ]]
    done
}
