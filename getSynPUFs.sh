# A simple loop to grab all the SynPUF data and unzip it

for num in {1..20}; do
    curl -o "scdm_v8_subsamples_$num.zip" "https://www.sentinelinitiative.org/sites/default/files/surveillance-tools/software-toolkit/scdm_v8_subsamples_$num.zip"
    curl -o "scdm_v8_diagnosis_$num.zip" "https://www.sentinelinitiative.org/sites/default/files/surveillance-tools/software-toolkit/scdm_v8_diagnosis_$num.zip"
    curl -o "scdm_v8_procedure_$num.zip" "https://www.sentinelinitiative.org/sites/default/files/surveillance-tools/software-toolkit/scdm_v8_procedure_$num.zip"
    
    unzip scdm_v8_subsamples_$num.zip
    unzip scdm_v8_diagnosis_$num.zip
    unzip scdm_v8_procedure_$num.zip

    mv subsamples_$num/*.* ./
    mv diagnosis_$num/*.* ./
    mv procedure_$num/*.* ./
    
    rm -r subsamples_$num
    rm -r diagnosis_$num
    rm -r procedure_$num
done