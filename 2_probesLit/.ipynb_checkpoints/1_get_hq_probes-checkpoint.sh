#!/bin/bash

#Submit this script with: sbatch thefilename

#SBATCH --time=8:00:00   # walltime
#SBATCH --ntasks=2   # number of tasks
#SBATCH --cpus-per-task=6   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH -p research   # partition(s)
#SBATCH --mem=250G   # memory per node
#SBATCH -J "OTcptest"   # job name
#SBATCH -e "errors.txt"
#SBATCH --mail-user=adasme@ebi.ac.uk   # email address
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL


# 1. Load the base Python and others
module load python/3.11.7
module load py-pyspark/3.3.1
module load py-psutil/5.9.5

# 2. Activate virtual environment
source /hps/software/users/chembl/adasme/cprobeslit_venv/bin/activate

# 4. Run 
echo "--- Starting pipeline ---"
python /nfs/production/arl/chembl/madasme/projects/chemical_probes_lit/2_probesLit/1_get_ner_hq_probes.py --matches /nfs/production/arl/chembl/madasme/projects/data/otner/matches --probes /nfs/production/arl/chembl/madasme/projects/ot_ner_probes/data/probesSubset.csv

echo "--- Pipeline Finished ---"


