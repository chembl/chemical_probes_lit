#Databases
import sqlite3
import pandas as pd

class ChemblDB():
    def __init__(self, path):
        self.cursor = self.connect(path)

    def connect(self, path):
        """Establishes the connection to the database and returns the cursor to perform further queries"""
        connection = sqlite3.connect(path)
        cursor = connection.cursor()
        return cursor
    
    def get_chemblid(self, inchi):
        """Given an inchikey it obtains the chemblid"""
        query = """
                SELECT s.standard_inchi_key, d.CHEMBL_ID
                FROM molecule_dictionary d
                JOIN compound_structures s on s.molregno = d.molregno
                WHERE s.standard_inchi_key = '{}'
                """
        
        result = self.cursor.execute(query.format(inchi)).fetchone()
        return result

    def get_synonyms(self, inchi):
        """Given a probes inchikey, it obtains the chembl synonyms"""
        query = """
        SELECT md.pref_name, ms.synonyms
        FROM MOLECULE_SYNONYMS ms
        JOIN MOLECULE_DICTIONARY md on md.molregno = ms.molregno
        JOIN COMPOUND_STRUCTURES cs on cs.molregno = md.molregno
        WHERE cs.standard_inchi_key = '{}'
        """
        result = self.cursor.execute(query.format(inchi)).fetchall()
        return result

    def get_pref_name(self, inchi):
        """Given a probes inchikey, it obtains the preferred name from CheMBL"""
        query = """
        SELECT md.pref_name
        FROM MOLECULE_DICTIONARY md
        JOIN COMPOUND_STRUCTURES cs on cs.molregno = md.molregno
        WHERE cs.standard_inchi_key = '{}'
        """
        self.cursor.execute(query.format(inchi))
        records = self.cursor.fetchone()
        return records

    def get_tar_synonyms(self, uniprot):
        """Given a probes inchikey, it obtains the chembl synonyms"""
        query = """
        SELECT cs.component_synonym, cs.syn_type
        FROM COMPONENT_SYNONYMS cs
        JOIN COMPONENT_SEQUENCES seq on seq.component_id = cs.component_id
        WHERE seq.accession = '{}'
        """
        self.cursor.execute(query.format(uniprot))
        records = self.cursor.fetchall()
        return records

    def get_all_units(self):
        """Retrieves a list ofunique units types in ChEMBL"""
        query = """
        SELECT distinct ac.UNITS, at.ASSAY_DESC
        FROM ACTIVITIES ac
        JOIN ASSAYS ass on ac.ASSAY_ID = ass.ASSAY_ID
        JOIN ASSAY_TYPE at on ass.ASSAY_TYPE = at.ASSAY_TYPE
        WHERE PCHEMBL_VALUE IS NOT NULL;
        """

        self.cursor.execute(query)
        records = self.cursor.fetchall()
        return records
    
    def get_all_docs(self):
        """Reteieves all pubmed ids of documents from which assays have been extracted"""
        query="""
        SELECT DISTINCT PUBMED_ID
        FROM DOCS 
        """

        records = self.cursor.execute(query).fetchone()
        return records
    
    def test(self):
        query = """
        SELECT name FROM sqlite_master WHERE type='table'
        """
        records = self.cursor.execute(query).fetchone()
        return records


class PnDDB():
    def __init__(self, path):
        self.cursor = self.connect(path)

    def connect(self,path):
        """Establishes the connection to the database and returns the cursor to perform further queries"""
        connection = sqlite3.connect(path)
        cursor = connection.cursor()
        return cursor
    
    def test(self):
        """Using the database cursor perfors the query to extract the golden set from Probes & Drugs and returns it as a pandas dataframe"""
        query = """
        SELECT name FROM sqlite_master WHERE type='table' AND name='probe'
        """
        print(query)
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        return records

    def get_chemblid(self, setid):
        """Given a Probes id, it obtains the chembl id"""
        query = """
        SELECT distinct pr.probeid,
        cp.name, cp.inchikey,
        edb.ligand_id
        FROM probe pr
        JOIN compound cp on pr.compound_id = cp.compoundid
        JOIN compoundtoexternaldb edb on edb.compound_id = cp.compoundid
        WHERE edb.externaldb_id = 2
        AND pr.compoundset_id = '{}';
        """

        self.cursor.execute(query.format(setid))
        records = self.cursor.fetchall()
        return records

    def get_synonyms(self, id):
        """Given a Probes name/id, it obtains all possible synonim names"""
        query = """
        SELECT distinct av.value, pr.probeid, cp.compoundid
        FROM probe pr
        JOIN compound cp on pr.compound_id = cp.compoundid
        JOIN compoundattribute ca on cp.compoundid = ca.compound_id
        JOIN attributevalue av on av.attributevalueid =  ca.value_id
        JOIN attribute aa on aa.attributeid = av.attribute_id
        JOIN attributetype at on at.attributetypeid = aa.type_id
        WHERE pr.probeid = '{}'
        AND aa.type_id = '3' 
        """
        
        self.cursor.execute(query.format(id))
        records = self.cursor.fetchall()
        return records
    
    def get_target_synonyms(self, id):
        """Given a target PnD id, it returns all synonyms"""
        query = """
        SELECT DISTINCT synonym 
        FROM targetsynonym 
        WHERE basetarget_id = '{}'
        """
        self.cursor.execute(query.format(id))
        records = self.cursor.fetchall()
        return records

    def get_main_set(self):
        """Using the database cursor perfors the query to extract the golden set from Probes & Drugs and returns it as a pandas dataframe"""
        query = """
        SELECT distinct pr.probeid, pr.compoundset_id, ta.uniprotid,
        cp.name, cs.name, cp.inchikey, bt.gene_name, 
        pr.control, pbt.basetarget_id
        FROM probe pr
        LEFT JOIN compound cp on pr.compound_id = cp.compoundid
        JOIN probetobasetarget pbt on pbt.probe_id = pr.probeid
        JOIN basetarget bt on bt.basetargetid = pbt.basetarget_id
        JOIN targettobasetarget tbt on tbt.basetarget_id = bt.basetargetid
        JOIN target ta on ta.targetid = tbt.target_id
        JOIN compoundset cs on cs.compoundsetid = pr.compoundset_id
        WHERE pr.origin_id != 'calculated'
        AND pr.compoundset_id IN ('28','213','408') 
        """
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        return records
    
    
class ProbesMainSet():
    def __init__(self, pnd, chembl):
        self.pnd = pnd
        self.chembl = chembl
        self.mainset = self.get_mainset()
        self.get_chemblid() #obtains mapping to chembl id by using inchikey
        self.probes_names = self.mainset.PROBE.unique().tolist()
        self.sets = self.mainset.SETID.unique().tolist()
        self.get_all_synonyms() #obtains synonims from different sources
        self.subset = None

    def get_mainset(self):
        """Using the database cursor performs the query to extract the golden set from Probes & Drugs and returns it as a pandas dataframe"""
        records = self.pnd.get_main_set()
        mainset = pd.DataFrame(records, columns = ['ID', 'SETID', 'UNIPROT', 'PROBE', 'SET', 'INCHI', 'GENE','CONTR', 'TARGETID'])
        mainset.CONTR = mainset.CONTR.apply(lambda x: None if x is None else x.strip("[]'")) #Clean control
        mainset.GENE = mainset.GENE.apply(lambda x: None if x is None else x.strip("[]'"))
        return mainset
    
    def get_chemblid(self):
        """Given the ainset it takes the inchikeys and obtains the mapping to bhemcl ids"""
        mydb = self.chembl
        mydf = self.mainset
        mapping = {}
        for inchi in mydf.INCHI.to_list():
            if inchi not in mapping:
                if mydb.get_chemblid(inchi):
                    mapping[inchi] = mydb.get_chemblid(inchi)[1]
        mydf['CHEMBLID'] = mydf.INCHI.map(mapping).fillna('None')

    def get_PnD_synonyms(self, probes):
        """Given a list of probes ids it recovers all possible synonyms from Probs&Drugs"""
        synoms = pd.DataFrame()
        sy = []
        for probe in probes:
            syns = self.pnd.get_synonyms(probe)
            sy.append([syn[0] for syn in syns])
        synoms['ID'] = probes
        synoms['SYNOMS'] = sy
        synoms.SYNOMS = synoms.SYNOMS.astype(str).replace("'", '', regex=True)
        return synoms

    def get_chembl_synonyms(self, probes):
        """Given a list of probes inchikeys recovers a list of all possible synonyms in ChEMBL"""
        synoms = pd.DataFrame()
        prefs = []
        sy = []
        for probe in probes:
            #Gets synonyms from the chembl database
            syns = self.chembl.get_synonyms(probe)
            if syns:
                syns = ', '.join([s[1] for s in syns]) #transform the list of tutples into one unique comma sep string
                sy.append(syns)
            else:
                sy.append(None)
            #Gets the prefferred name from the ChEMBL database
            pref = self.chembl.get_pref_name(probe)
            if pref:
                prefs.append(pref[0])
            else:
                prefs.append(None)
        synoms['INCHI'] = probes
        synoms['PREF_NAME'] = prefs
        synoms['SYNOMS_CHEMBL'] = sy
        return synoms

    def get_PnD_target_synonims(self, targetids):
        """Given a list of target ids (from PnD) it finds their targets synonims"""
        synoms = pd.DataFrame()
        sy = []
        for id in targetids:
            syns = self.pnd.get_target_synonyms(id)
            if syns:
                syns = ', '.join([s[0] for s in syns])
                sy.append(syns)
            else:
                sy.append(None)
        synoms['TARGETID'] = targetids
        synoms['SYNOMS_TARGET'] = sy
        return synoms

    def get_chembl_target_synonims(self, uniprots):
        """Given the main dataset of probes it obtains the probes' targets synonyms from different sources
        and merge them all into an unique list"""
        synoms = pd.DataFrame()
        sy = []
        for uniprot in uniprots:
            syns = self.chembl.get_tar_synonyms(uniprot)
            if syns:
                syns =', '.join([s[0] for s in syns])#transform the list of tutples into one unique comma sep string
                sy.append(syns)
            else:
                sy.append(None)
        synoms['UNIPROT'] = uniprots
        synoms['SYNOMS_TARGET_CHEMBL'] = sy
        return synoms
    
    def clean_synonyms(self):
        """Merge synonims from Probes&Drugs and Chembl keeping uniques"""
        synoms = []
        synomstar = []
        pr_syns = []
        ch_syns = []
        tar_syns=[]
        ch_tar_syns = []

        for row in self.mainset.itertuples():
            if row.SYNOMS != None:
                pr_syns = list(filter(lambda x: x != None, row.SYNOMS.strip('[]').split(', '))) #Cleans the syns list
            if row.SYNOMS_CHEMBL != None:
                ch_syns = list(filter(lambda x: x != None, row.SYNOMS_CHEMBL.strip('[]').split(', '))) #Cleans the syns list
            #if row.SYNOMS_CHEBI != None:
            #    chebi_syns = list(filter(lambda x: x != None, row.SYNOMS_CHEBI.strip('[]').split(', '))) #Cleans the syns list
            synoms.append(', '.join(list(set(pr_syns + ch_syns)))) #merge compound synonims and gets list of uniques values as string
            #synoms.append(', '.join(list(set(pr_syns + ch_syns + chebi_syns)))) #merge compound synonims and gets list of uniques values as string
            if row.SYNOMS_TARGET != None:
                tar_syns = list(filter(lambda x: x != None, row.SYNOMS_TARGET.strip('[]').split(', '))) #Cleans the syns list
            if row.SYNOMS_TARGET_CHEMBL != None:
                ch_tar_syns = list(filter(lambda x: x != None, row.SYNOMS_TARGET_CHEMBL.strip('[]').split(', '))) #Cleans the syns list
            synomstar.append(', '.join(list(set(tar_syns + ch_tar_syns)))) #merge target synonims and gets list of uniques values as string
        self.mainset = self.mainset.drop(['SYNOMS', 'SYNOMS_CHEMBL', 'SYNOMS_TARGET', 'SYNOMS_TARGET_CHEMBL'], axis=1) #Delete independent columns
        #self.mainset = self.mainset.drop(['SYNOMS', 'SYNOMS_CHEMBL', 'SYNOMS_CHEBI', 'SYNOMS_TARGET', 'SYNOMS_TARGET_CHEMBL'], axis=1) #Delete independent columns
        self.mainset['SYNOMS'] = synoms #keep unique column with merged data
        self.mainset['SYNOMS_TARGET'] = synomstar #keep unique column with merged data

    def get_all_synonyms(self):
        """Given the main dataset of probes it obtains the probes synonyms from different sources
        and merge them all into an unique list"""
        #Obtains the synonims for the probes in mainset (from Probes&Drugs)
        probes_syn = self.get_PnD_synonyms(self.mainset.ID.unique())
        self.mainset = self.mainset.merge(probes_syn, on='ID')
        #Obtains the ChEMBL pref name and synonims for the probes in mainset (from Chembl)
        chembl_syn = self.get_chembl_synonyms(self.mainset.INCHI.unique())
        self.mainset = self.mainset.merge(chembl_syn, on='INCHI')
        #Obtains the ChEBI synonims for the probes in mainset (from Chembl)
        #chebi_syn = self.get_chebi_synonyms(self.mainset.INCHI.unique())
        #self.mainset = self.mainset.merge(chebi_syn, on='INCHI')
        #Obtains the PnD target synonyms
        targets_syn = self.get_PnD_target_synonims(self.mainset.TARGETID.unique())
        self.mainset = self.mainset.merge(targets_syn, on='TARGETID')
        #Obtains the ChEMBL target synonyms
        chembl_tar_syn = self.get_chembl_target_synonims(self.mainset.UNIPROT.unique())
        self.mainset = self.mainset.merge(chembl_tar_syn, on='UNIPROT')
    
    def clean_data(self):
        self.clean_synonyms() #merge all found synonims removing duplicates
        self.mainset = self.mainset.drop(['TARGETID'], axis=1) #Delete targetsid from PnD creating duplicates (same uniprot but different datapoint)
        self.mainset = self.mainset.drop_duplicates() #removes duplicated rows
        self.mainset = self.mainset.drop(['ID'], axis=1)

    def write_mainset(self, path):
        self.mainset.to_csv(path, sep="\t", index=False)
    
    def write_subset(self, path):
        self.subset.to_csv(path, sep="\t", index=False)
    