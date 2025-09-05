#!/bin/bash

#Submit this script with: sbatch thefilename

#SBATCH --time=8:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=1   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH -p research   # partition(s)
#SBATCH --mem=5G   # memory per node
#SBATCH -J "OTcptest_3"   # job name
#SBATCH -e "errors_3.txt"
#SBATCH --mail-user=adasme@ebi.ac.uk   # email address
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL


# 1. Load the base Python and others
module load python/3.11.7
module load py-psutil/5.9.5

# 2. Activate virtual environment
source /hps/software/users/chembl/adasme/cprobeslit_venv/bin/activate

# 4. Run 
echo "--- Starting pipeline 2 ---"
python /nfs/production/arl/chembl/madasme/projects/chemical_probes_lit/2_probesLit/3_filter_targets.py --input /nfs/production/arl/chembl/madasme/projects/data/testing/data/2_ner_probes_triplets.csv --probes /nfs/production/arl/chembl/madasme/projects/ot_ner_probes/data/probesSubset.csv

echo "--- Pipeline 2 Finished ---"


