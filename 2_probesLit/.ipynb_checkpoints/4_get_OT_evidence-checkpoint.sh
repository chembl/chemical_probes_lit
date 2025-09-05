#!/bin/bash

#Submit this script with: sbatch thefilename

#SBATCH --time=8:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=4   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH -p research   # partition(s)
#SBATCH --mem=12G   # memory per node
#SBATCH -J "OTcptest_4"   # job name
#SBATCH -e "errors_4.txt"
#SBATCH --mail-user=adasme@ebi.ac.uk   # email address
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL


# 1. Load the base Python and others
module load python/3.11.7
module load py-psutil/5.9.5
module load py-pyspark/3.3.1

# 2. Activate virtual environment
source /hps/software/users/chembl/adasme/cprobeslit_venv/bin/activate

# 4. Run 
echo "--- Starting pipeline 2 ---"
python /nfs/production/arl/chembl/madasme/projects/chemical_probes_lit/2_probesLit/4_get_OT_evidence.py --input /nfs/production/arl/chembl/madasme/projects/data/testing/data/3_ner_probes_triplets_ptpairs.csv --evidence /nfs/production/arl/chembl/madasme/projects/data/associationByDatasourceIndirect
echo "--- Pipeline 2 Finished ---"


