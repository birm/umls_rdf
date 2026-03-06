# high level concepts and narrower versions
# diabetes
python omop_umls_analysis.py 201820 "Diabetes mellitus"
# (specific) type 1
python omop_umls_analysis.py 201254 "Type 1 Diabetes mellitus"

# drugs are messy!!
# ibuprofen
python omop_umls_analysis.py 1177480 "Ibuprofen"
#  penicilin G
python omop_umls_analysis.py 1728416 "penicilin G"

# Cardiovascular procedures
python omop_umls_analysis.py 4012185 "Cardiovascular procedures"
# (specific) removal of blood clot from shunt
python omop_umls_analysis.py 4002167 "removal of blood clot from shunt"

# dx
python omop_umls_analysis.py 316866 "hypertension"
python omop_umls_analysis.py 432867 "Hyperlipidemia"
python omop_umls_analysis.py 201826 "Type 2 diabetes mellitus"
python omop_umls_analysis.py 77670 "Chest pain"
python omop_umls_analysis.py 313217 "Atrial fibrillation"


# rx 
python omop_umls_analysis.py 967823 "sodium chloride"
python omop_umls_analysis.py 19097059 "dextrose solution"
python omop_umls_analysis.py 19076324 "glucose injectible"
python omop_umls_analysis.py 1154029 "fentanyl"
python omop_umls_analysis.py 4285892 "Insulin"

# proc
# NOTE: had to convert some from CPT to snomed due to not licensing CPT
python omop_umls_analysis.py 4240345 "Physical examination"
python omop_umls_analysis.py 4289459 "inpatient visit"
python omop_umls_analysis.py 4145308 "12 lead ECG"
python omop_umls_analysis.py 4163872 "X-ray of chest"
python omop_umls_analysis.py 4132855 "immunization"

# make comparison outputs

# high level concepts and narrower versions
# diabetes
python review_analysis_results.py 201820 "Diabetes mellitus"
# (specific) type 1
python review_analysis_results.py 201254 "Type 1 Diabetes mellitus"

# drugs are messy!!
# ibuprofen
python review_analysis_results.py 1177480 "Ibuprofen"
#  penicilin G
python review_analysis_results.py 1728416 "penicilin G"

# Cardiovascular procedures
python review_analysis_results.py 4012185 "Cardiovascular procedures"
# (specific) removal of blood clot from shunt
python review_analysis_results.py 4002167 "removal of blood clot from shunt"

# dx
python review_analysis_results.py 316866 "hypertension"
python review_analysis_results.py 432867 "Hyperlipidemia"
python review_analysis_results.py 201826 "Type 2 diabetes mellitus"
python review_analysis_results.py 77670 "Chest pain"
python review_analysis_results.py 313217 "Atrial fibrillation"


# rx 
python review_analysis_results.py 967823 "sodium chloride"
python review_analysis_results.py 19097059 "dextrose solution"
python review_analysis_results.py 19076324 "glucose injectible"
python review_analysis_results.py 1154029 "fentanyl"
python review_analysis_results.py 4285892 "Insulin"

# proc
# NOTE: had to convert some from CPT to snomed due to not licensing CPT
python review_analysis_results.py 4240345 "Physical examination"
python review_analysis_results.py 4289459 "inpatient visit"
python review_analysis_results.py 4145308 "12 lead ECG"
python review_analysis_results.py 4163872 "X-ray of chest"
python review_analysis_results.py 4132855 "immunization"