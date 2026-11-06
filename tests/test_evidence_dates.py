import os
from pathlib import Path
import sys
import unittest
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / '2_probesLit'))
from helpers.evidence_dates import release_year, association_dates, dated_records, summarize_dates, join_date_summary
from helpers.temporal_analysis import selected_years, add_temporal_years


class TemporalTests(unittest.TestCase):
    def test_pinned_release(self):
        self.assertEqual(release_year('26.09'), 2026)
        with self.assertRaises(ValueError):
            release_year('latest')

    def test_missing_dates_not_zero_and_no_array_fallback(self):
        frame = pd.DataFrame({'targetId':['T','T'], 'diseaseId':['D','D'],
                              'date':['2000-01-01','2002-01-01'], 'firstEvidenceYear':[None,2005]})
        result = add_temporal_years(frame)
        self.assertTrue(pd.isna(result.firstEvidence.iloc[0]))
        self.assertEqual(result.firstEvidenceAll.tolist(),[2005,2005])
        with self.assertRaisesRegex(ValueError,'predates'):
            selected_years(pd.DataFrame({'allpublicationYears':['[2000]']}))


@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS') == '1', 'Requires Java/PySpark')
class DateSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master('local[2]').appName('date-policy-tests').config('spark.sql.shuffle.partitions','2').getOrCreate()
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_record_fallback_is_per_record_not_global_min(self):
        # SQL fixtures avoid Python-worker serialization on Spark 3.3/Python 3.11.
        frame = self.spark.sql("""SELECT * FROM VALUES
          ('D','T','eva','2010','1990','1980',NULL),
          ('D','T','eva',NULL,'2012','1970',NULL),
          ('D','T','eva',NULL,NULL,'2014',NULL),
          ('D','T','eva',NULL,NULL,NULL,'2015-04-01'),
          ('D','T','eva','invalid',NULL,NULL,NULL),
          ('D','T','eva','2027',NULL,NULL,NULL)
          AS e(diseaseId,targetId,datasourceId,publicationYear,curationYear,studyYear,evidenceDate)""")
        records, policy = dated_records(frame,2026)
        summary = summarize_dates(records,policy)
        row = summary.first()
        self.assertEqual(row.firstEvidenceYear,2010)
        self.assertEqual(row.allEvidenceYears,[2010,2012,2014,2015])
        self.assertEqual(row.earliestYearOrigins,['publicationYear'])
        self.assertEqual(row.datedEvidenceCount,4)
        self.assertEqual(row.undatedEvidenceCount,2)
        consolidated, policy = dated_records(frame.select('diseaseId','targetId','datasourceId','evidenceDate'),2026)
        self.assertEqual(policy,'record:OT_evidenceDate')
        self.assertEqual(summarize_dates(consolidated,policy).first().firstEvidenceYear,2015)

    def test_timeseries_padding_nulls_and_preserved_rows(self):
        frame = self.spark.sql("""SELECT 'D' diseaseId, 'T' targetId,
        'datasourceId' aggregationType, 'eva' aggregationValue,
        array(named_struct('year',1995,'yearlyEvidenceCount',0),
              named_struct('year',2000,'yearlyEvidenceCount',2),
              named_struct('year',2001,'yearlyEvidenceCount',NULL),
              named_struct('year',NULL,'yearlyEvidenceCount',3),
              named_struct('year',2027,'yearlyEvidenceCount',1)) timeseries""")
        summary = association_dates(frame,2026)
        row=summary.first()
        self.assertEqual(row.firstEvidenceYear,2000)
        self.assertEqual(row.allEvidenceYears,[2000])
        self.assertEqual(row.datedEvidenceCount,2)
        self.assertEqual(row.undatedEvidenceCount,4)
        probes=self.spark.sql("""SELECT * FROM VALUES
          ('D','T','eva'),('D','T','eva'),('other','T','eva'),('D','T','impc'),('D','T',NULL)
          AS p(diseaseId,targetId,datasourceId)""")
        result=join_date_summary(probes,summary).collect()
        self.assertEqual(len(result),5)
        self.assertEqual(sorted(r.datedEvidenceStatus for r in result),
                         ['matched','matched','no_association','pair_absent','source_absent'])
        self.assertTrue(all(r.allpublicationYears is None for r in result))
        empty_summary = association_dates(frame.limit(0),2026)
        empty_results = join_date_summary(probes, empty_summary, summary.select('datasourceId')).collect()
        self.assertEqual(sum(r.datedEvidenceStatus == 'pair_absent' for r in empty_results),3)
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            association_dates(frame.unionByName(frame),2026)

    def test_only_undated_evidence_is_not_early(self):
        frame=self.spark.sql("""SELECT 'D' diseaseId, 'T' targetId,
        'datasourceId' aggregationType, 'eva' aggregationValue,
        array(named_struct('year',cast(NULL as int),'yearlyEvidenceCount',3)) timeseries""")
        result=join_date_summary(frame.selectExpr('diseaseId','targetId','aggregationValue as datasourceId'),association_dates(frame,2026)).first()
        self.assertEqual(result.datedEvidenceStatus,'dates_missing')
        self.assertIsNone(result.firstEvidenceYear)
        self.assertIsNone(result.allEvidenceYears)

if __name__=='__main__': unittest.main()
