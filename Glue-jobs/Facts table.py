import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Processed
Processed_node1775697333887 = glueContext.create_dynamic_frame.from_catalog(database="freelancing", table_name="part_00000_e160abe1_b7b6_4c26_a097_2eebb1eb3c87_c000_snappy_parquet", transformation_ctx="Processed_node1775697333887")

# Script generated for node Skill
Skill_node1775697258684 = glueContext.create_dynamic_frame.from_catalog(database="freelancing", table_name="skill", transformation_ctx="Skill_node1775697258684")

# Script generated for node Language
Language_node1775697049056 = glueContext.create_dynamic_frame.from_catalog(database="freelancing", table_name="language", transformation_ctx="Language_node1775697049056")

# Script generated for node Location
Location_node1775697252768 = glueContext.create_dynamic_frame.from_catalog(database="freelancing", table_name="location", transformation_ctx="Location_node1775697252768")

# Script generated for node Freelancer
Freelancer_node1775696958964 = glueContext.create_dynamic_frame.from_catalog(database="freelancing", table_name="freelancer", transformation_ctx="Freelancer_node1775696958964")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1775698121360 = ApplyMapping.apply(frame=Skill_node1775697258684, mappings=[("skill_id", "long", "right_skill_id", "long"), ("primary_skill", "string", "right_primary_skill", "string")], transformation_ctx="RenamedkeysforJoin_node1775698121360")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1775697800736 = ApplyMapping.apply(frame=Language_node1775697049056, mappings=[("language_id", "long", "right_language_id", "long"), ("language", "string", "right_language", "string")], transformation_ctx="RenamedkeysforJoin_node1775697800736")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1775697970331 = ApplyMapping.apply(frame=Location_node1775697252768, mappings=[("location_id", "long", "right_location_id", "long"), ("country", "string", "right_country", "string")], transformation_ctx="RenamedkeysforJoin_node1775697970331")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1775697429786 = ApplyMapping.apply(frame=Freelancer_node1775696958964, mappings=[("freelancer_id", "long", "right_freelancer_id", "long"), ("name", "string", "right_name", "string"), ("gender", "string", "right_gender", "string"), ("age", "int", "right_age", "int")], transformation_ctx="RenamedkeysforJoin_node1775697429786")

# Script generated for node Join
Join_node1775697411054 = Join.apply(frame1=Processed_node1775697333887, frame2=RenamedkeysforJoin_node1775697429786, keys1=["name", "gender", "age"], keys2=["right_name", "right_gender", "right_age"], transformation_ctx="Join_node1775697411054")

# Script generated for node Join
Join_node1775697778458 = Join.apply(frame1=Join_node1775697411054, frame2=RenamedkeysforJoin_node1775697800736, keys1=["language"], keys2=["right_language"], transformation_ctx="Join_node1775697778458")

# Script generated for node Join
Join_node1775697956268 = Join.apply(frame1=Join_node1775697778458, frame2=RenamedkeysforJoin_node1775697970331, keys1=["country"], keys2=["right_country"], transformation_ctx="Join_node1775697956268")

# Script generated for node Join
Join_node1775698093040 = Join.apply(frame1=Join_node1775697956268, frame2=RenamedkeysforJoin_node1775698121360, keys1=["primary_skill"], keys2=["right_primary_skill"], transformation_ctx="Join_node1775698093040")

# Script generated for node Select Fields
SelectFields_node1775698236283 = SelectFields.apply(frame=Join_node1775698093040, paths=["right_freelancer_id", "right_skill_id", "right_language_id", "right_location_id", "is_active", "`hourly_rate (usd)`", "`client_satisfaction (%)`", "rating", "years_of_experience"], transformation_ctx="SelectFields_node1775698236283")

# Script generated for node Change Schema
ChangeSchema_node1775698547989 = ApplyMapping.apply(frame=SelectFields_node1775698236283, mappings=[("right_freelancer_id", "long", "Freelancer ID", "long"), ("right_skill_id", "long", "Skill ID", "long"), ("right_language_id", "long", "Language ID", "long"), ("right_location_id", "long", "Location ID", "long"), ("is_active", "string", "is_active", "string"), ("`hourly_rate (usd)`", "int", "`hourly_rate (usd)`", "int"), ("`client_satisfaction (%)`", "int", "`client_satisfaction (%)`", "int"), ("rating", "double", "rating", "double"), ("years_of_experience", "int", "years_of_experience", "int")], transformation_ctx="ChangeSchema_node1775698547989")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1775698547989, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775699703907", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (ChangeSchema_node1775698547989.count() >= 1):
   ChangeSchema_node1775698547989 = ChangeSchema_node1775698547989.coalesce(1)
AmazonS3_node1775699751711 = glueContext.getSink(path="s3://myfirst-bucket-126606499301-eu-north-1-an/Processed-Data/Fact-Table/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1775699751711")
AmazonS3_node1775699751711.setCatalogInfo(catalogDatabase="freelancing",catalogTableName="Facts_Table")
AmazonS3_node1775699751711.setFormat("glueparquet", compression="snappy")
AmazonS3_node1775699751711.writeFrame(ChangeSchema_node1775698547989)
job.commit()