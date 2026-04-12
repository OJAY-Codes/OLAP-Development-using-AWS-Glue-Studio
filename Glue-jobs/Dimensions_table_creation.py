import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node Adding id
def AddLanguageID(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import monotonically_increasing_id
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # Step 1: Get the key
    key = list(dfc.keys())[0]

    # Step 2: Select the DynamicFrame
    dynamic_frame = dfc.select(key)

    # Step 3: Convert to DataFrame
    df = dynamic_frame.toDF()

    # Step 4: Apply transformation
    df = df.withColumn("Language_id", monotonically_increasing_id())

    # Step 5: Convert back to DynamicFrame
    result = DynamicFrame.fromDF(df, glueContext, "result")

    return DynamicFrameCollection({"result": result}, glueContext)
# Script generated for node Adding id
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import monotonically_increasing_id
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # Step 1: Get the key
    key = list(dfc.keys())[0]

    # Step 2: Select the DynamicFrame
    dynamic_frame = dfc.select(key)

    # Step 3: Convert to DataFrame
    df = dynamic_frame.toDF()

    # Step 4: Apply transformation
    df = df.withColumn("Freelancer_id", monotonically_increasing_id())

    # Step 5: Convert back to DynamicFrame
    result = DynamicFrame.fromDF(df, glueContext, "result")

    return DynamicFrameCollection({"result": result}, glueContext)
# Script generated for node Adding id
def MyLocation(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import monotonically_increasing_id
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # Step 1: Get the key
    key = list(dfc.keys())[0]

    # Step 2: Select the DynamicFrame
    dynamic_frame = dfc.select(key)

    # Step 3: Convert to DataFrame
    df = dynamic_frame.toDF()

    # Step 4: Apply transformation
    df = df.withColumn("Location_id", monotonically_increasing_id())

    # Step 5: Convert back to DynamicFrame
    result = DynamicFrame.fromDF(df, glueContext, "result")

    return DynamicFrameCollection({"result": result}, glueContext)
# Script generated for node Adding id
def AddSkill(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import monotonically_increasing_id
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # Step 1: Get the key
    key = list(dfc.keys())[0]

    # Step 2: Select the DynamicFrame
    dynamic_frame = dfc.select(key)

    # Step 3: Convert to DataFrame
    df = dynamic_frame.toDF()

    # Step 4: Apply transformation
    df = df.withColumn("Skill_id", monotonically_increasing_id())

    # Step 5: Convert back to DynamicFrame
    result = DynamicFrame.fromDF(df, glueContext, "result")

    return DynamicFrameCollection({"result": result}, glueContext)
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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1775246723806 = glueContext.create_dynamic_frame.from_catalog(database="freelancing", table_name="part_00000_e160abe1_b7b6_4c26_a097_2eebb1eb3c87_c000_snappy_parquet", transformation_ctx="AWSGlueDataCatalog_node1775246723806")

# Script generated for node Language
Language_node1775246930889 = SelectFields.apply(frame=AWSGlueDataCatalog_node1775246723806, paths=["language"], transformation_ctx="Language_node1775246930889")

# Script generated for node Freelancers
Freelancers_node1775246764148 = SelectFields.apply(frame=AWSGlueDataCatalog_node1775246723806, paths=["name", "gender", "age"], transformation_ctx="Freelancers_node1775246764148")

# Script generated for node Skill
Skill_node1775246988258 = SelectFields.apply(frame=AWSGlueDataCatalog_node1775246723806, paths=["primary_skill"], transformation_ctx="Skill_node1775246988258")

# Script generated for node Location
Location_node1775246809075 = SelectFields.apply(frame=AWSGlueDataCatalog_node1775246723806, paths=["country"], transformation_ctx="Location_node1775246809075")

# Script generated for node Drop Duplicates
DropDuplicates_node1775246964488 =  DynamicFrame.fromDF(Language_node1775246930889.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1775246964488")

# Script generated for node Drop Duplicates
DropDuplicates_node1775246878092 =  DynamicFrame.fromDF(Freelancers_node1775246764148.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1775246878092")

# Script generated for node Drop Duplicates
DropDuplicates_node1775247040959 =  DynamicFrame.fromDF(Skill_node1775246988258.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1775247040959")

# Script generated for node Drop Duplicates
DropDuplicates_node1775246845161 =  DynamicFrame.fromDF(Location_node1775246809075.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1775246845161")

# Script generated for node Adding id
Addingid_node1775248882911 = AddLanguageID(glueContext, DynamicFrameCollection({"DropDuplicates_node1775246964488": DropDuplicates_node1775246964488}, glueContext))

# Script generated for node Adding id
Addingid_node1775247528887 = MyTransform(glueContext, DynamicFrameCollection({"DropDuplicates_node1775246878092": DropDuplicates_node1775246878092}, glueContext))

# Script generated for node Adding id
Addingid_node1775248968069 = AddSkill(glueContext, DynamicFrameCollection({"DropDuplicates_node1775247040959": DropDuplicates_node1775247040959}, glueContext))

# Script generated for node Adding id
Addingid_node1775248782179 = MyLocation(glueContext, DynamicFrameCollection({"DropDuplicates_node1775246845161": DropDuplicates_node1775246845161}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1775255869276 = SelectFromCollection.apply(dfc=Addingid_node1775248882911, key=list(Addingid_node1775248882911.keys())[0], transformation_ctx="SelectFromCollection_node1775255869276")

# Script generated for node Select From Collection
SelectFromCollection_node1775255471413 = SelectFromCollection.apply(dfc=Addingid_node1775247528887, key=list(Addingid_node1775247528887.keys())[0], transformation_ctx="SelectFromCollection_node1775255471413")

# Script generated for node Select From Collection
SelectFromCollection_node1775256010860 = SelectFromCollection.apply(dfc=Addingid_node1775248968069, key=list(Addingid_node1775248968069.keys())[0], transformation_ctx="SelectFromCollection_node1775256010860")

# Script generated for node Select From Collection
SelectFromCollection_node1775255657129 = SelectFromCollection.apply(dfc=Addingid_node1775248782179, key=list(Addingid_node1775248782179.keys())[0], transformation_ctx="SelectFromCollection_node1775255657129")

# Script generated for node Select Fields
SelectFields_node1775255890013 = SelectFields.apply(frame=SelectFromCollection_node1775255869276, paths=["Language_id", "language"], transformation_ctx="SelectFields_node1775255890013")

# Script generated for node Select Fields
SelectFields_node1775255506059 = SelectFields.apply(frame=SelectFromCollection_node1775255471413, paths=["Freelancer_id", "name", "gender", "age"], transformation_ctx="SelectFields_node1775255506059")

# Script generated for node Select Fields
SelectFields_node1775256021631 = SelectFields.apply(frame=SelectFromCollection_node1775256010860, paths=["Skill_id", "primary_skill"], transformation_ctx="SelectFields_node1775256021631")

# Script generated for node Select Fields
SelectFields_node1775255679612 = SelectFields.apply(frame=SelectFromCollection_node1775255657129, paths=["Location_id", "country"], transformation_ctx="SelectFields_node1775255679612")

# Script generated for node Language
EvaluateDataQuality().process_rows(frame=SelectFields_node1775255890013, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775245637018", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SelectFields_node1775255890013.count() >= 1):
   SelectFields_node1775255890013 = SelectFields_node1775255890013.coalesce(1)
Language_node1775255936031 = glueContext.getSink(path="s3://myfirst-bucket-126606499301-eu-north-1-an/Processed-Data/Language/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Language_node1775255936031")
Language_node1775255936031.setCatalogInfo(catalogDatabase="freelancing",catalogTableName="Language")
Language_node1775255936031.setFormat("glueparquet", compression="snappy")
Language_node1775255936031.writeFrame(SelectFields_node1775255890013)
# Script generated for node Freelancer
EvaluateDataQuality().process_rows(frame=SelectFields_node1775255506059, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775245637018", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SelectFields_node1775255506059.count() >= 1):
   SelectFields_node1775255506059 = SelectFields_node1775255506059.coalesce(1)
Freelancer_node1775255555367 = glueContext.getSink(path="s3://myfirst-bucket-126606499301-eu-north-1-an/Processed-Data/Freelancer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Freelancer_node1775255555367")
Freelancer_node1775255555367.setCatalogInfo(catalogDatabase="freelancing",catalogTableName="Freelancer")
Freelancer_node1775255555367.setFormat("glueparquet", compression="snappy")
Freelancer_node1775255555367.writeFrame(SelectFields_node1775255506059)
# Script generated for node Skill
EvaluateDataQuality().process_rows(frame=SelectFields_node1775256021631, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775245637018", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SelectFields_node1775256021631.count() >= 1):
   SelectFields_node1775256021631 = SelectFields_node1775256021631.coalesce(1)
Skill_node1775256096727 = glueContext.getSink(path="s3://myfirst-bucket-126606499301-eu-north-1-an/Processed-Data/Skills/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Skill_node1775256096727")
Skill_node1775256096727.setCatalogInfo(catalogDatabase="freelancing",catalogTableName="Skill")
Skill_node1775256096727.setFormat("glueparquet", compression="snappy")
Skill_node1775256096727.writeFrame(SelectFields_node1775256021631)
# Script generated for node Location
EvaluateDataQuality().process_rows(frame=SelectFields_node1775255679612, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775245637018", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SelectFields_node1775255679612.count() >= 1):
   SelectFields_node1775255679612 = SelectFields_node1775255679612.coalesce(1)
Location_node1775255723616 = glueContext.getSink(path="s3://myfirst-bucket-126606499301-eu-north-1-an/Processed-Data/Location/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Location_node1775255723616")
Location_node1775255723616.setCatalogInfo(catalogDatabase="freelancing",catalogTableName="Location")
Location_node1775255723616.setFormat("glueparquet", compression="snappy")
Location_node1775255723616.writeFrame(SelectFields_node1775255679612)
job.commit()