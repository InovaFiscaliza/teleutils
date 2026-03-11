# Pasta base origem
INPUT_FOLDER = "/data/cdr/chamadas_abusivas/cdr_processado/Semana86"

# Pasta de arquivos extraídos
EXTRACTED_FOLDER = "/data/cdr/chamadas_abusivas/cdr_extraido/Semana86"

# Pasta de arquivos transformados
TRANSFORMED_FOLDER = "/data/cdr/chamadas_abusivas/cdr_transformado/Semana86"

cdr_files = {
    "claro_ericsson": {
        "cdr_processado": f"{INPUT_FOLDER}/CLARO*Ericsson",
        "cdr_extraido": f"{EXTRACTED_FOLDER}/claro_ericsson_extraido.parquet",
        "cdr_transformado": f"{TRANSFORMED_FOLDER}/claro_ericsson_transformado.parquet",
    },
    "tim_ericsson": {
        "cdr_processado": f"{INPUT_FOLDER}/TIM*Ericsson",
        "cdr_extraido": f"{EXTRACTED_FOLDER}/tim_ericsson_extraido.parquet",
        "cdr_transformado": f"{TRANSFORMED_FOLDER}/tim_ericsson_transformado.parquet",
    },
    "vivo_ericsson": {
        "cdr_processado": f"{INPUT_FOLDER}/VIVO*Ericsson",
        "cdr_extraido": f"{EXTRACTED_FOLDER}/vivo_ericsson_extraido.parquet",
        "cdr_transformado": f"{TRANSFORMED_FOLDER}/vivo_ericsson_transformado.parquet",
    },
    "tim_volte": {
        "cdr_processado": f"{INPUT_FOLDER}/TIM*Volte",
        "cdr_extraido": f"{EXTRACTED_FOLDER}/tim_volte_extraido.parquet",
        "cdr_transformado": f"{TRANSFORMED_FOLDER}/tim_volte_transformado.parquet",
    },
    "tim_stir": {
        "cdr_processado": f"{INPUT_FOLDER}/TIM*Stir",
        "cdr_extraido": f"{EXTRACTED_FOLDER}/tim_stir_extraido.parquet",
        "cdr_transformado": f"{TRANSFORMED_FOLDER}/tim_stir_transformado.parquet",
    },
    "vivo_volte": {
        "cdr_processado": f"{INPUT_FOLDER}/VivoVolteConsolidado",
        "cdr_extraido": f"{EXTRACTED_FOLDER}/vivo_volte_extraido.parquet",
        "cdr_transformado": f"{TRANSFORMED_FOLDER}/vivo_volte_transformado.parquet",
    },
}
