### Author: Melissa F. Adasme ###
#Defines the class for ChEMBL database 

#from config import *
import sqlite3


class ChemblDB():
    def __init__(self,chemblpath):
        self.cursor = self.connect(chemblpath)

    def connect(self, chemblpath):
        """Establishes the connection to the database and returns the cursor to perform further queries"""
        connection = sqlite3.connect(chemblpath)
        cursor = connection.cursor()
        return cursor
    
    def get_chemblid(self, inchi):
        """Given an inchikey it obtains the chemblid"""
        query = """
                SELECT d.CHEMBL_ID
                FROM molecule_dictionary d
                JOIN compound_structures s on s.molregno = d.molregno
                WHERE s.standard_inchi_key = '{}'
                """
        
        result = self.cursor.execute(query.format(inchi)).fetchone()
        return result
    
    def get_max_phase(self, inchi):
        """Given an inchikey it obtains the max phase in chembl"""
        query = """
                SELECT d.MAX_PHASE
                FROM molecule_dictionary d
                JOIN compound_structures s on s.molregno = d.molregno
                WHERE s.standard_inchi_key ='GNETVUVZFYJATO-UHFFFAOYSA-N'
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

    def check_if_in_chembl(self, pubmed):
        query="""
        select distinct DOC_ID, CHEMBL_ID
        from DOCS
        where PUBMED_ID = '{}'
        """
        self.cursor.execute(query.format(pubmed))
        records = self.cursor.fetchall()
        return records
