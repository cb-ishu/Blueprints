from common.methods import set_progress
from infrastructure.models import Environment
from resourcehandlers.aws.models import AWSHandler
from utilities.logger import ThreadLogger

logger = ThreadLogger(__name__)

def get_aws_rh_and_region(resource):
    rh_aws_id = resource.aws_rh_id
    aws_region =  resource.aws_region
    rh_aws = None

    if rh_aws_id is not None or rh_aws_id != "":
        rh_aws = AWSHandler.objects.filter(id=rh_aws_id).first()

    return aws_region, rh_aws

def run(job, logger=None, **kwargs):
    resource = kwargs.pop('resources').first()

    set_progress(f"PostgreSQL database Delete plugin running for resource: {resource}")
    logger.info(f"PostgreSQL database Delete plugin running for resource: {resource}")
    
    postgresql_database_identifier = resource.db_identifier
    
    # get aws region and resource handler object
    aws: AWSHandler
    region, aws, = get_aws_rh_and_region(resource)

    if aws is None or aws == "":
        return "WARNING", "", "Need a valid aws region to delete this database"

    set_progress('Connecting to Amazon PostgreSQL Database')
    
    # initialize boto3 client
    client = aws.get_boto3_client(region, 'rds')
    
    try:
        # verify PostgreSQL database instance
        rds_resp = client.describe_db_instances(DBInstanceIdentifier=postgresql_database_identifier)
    except Exception as err:
        if "DBInstanceNotFound" in str(err):
            return "WARNING", f"PostgreSQL Database instance {postgresql_database_identifier} not found, it may have already been deleted", ""
        raise RuntimeError(err)
    
    job.set_progress('Deleting PostgreSQL Database {0}...'.format(postgresql_database_identifier))
    
    # delete PostgreSQL Database from AWS
    client.delete_db_instance(
        DBInstanceIdentifier=postgresql_database_identifier,
        # AWS strongly recommends taking a final snapshot before deleting a DB.
        # To do so, either set this to False or let the user choose by making it
        # a runtime action input (in that case be sure to set the param type to
        # Boolean so users get a dropdown).
        SkipFinalSnapshot=True,
        DeleteAutomatedBackups=True,
    )
    
    job.set_progress(f"PostgreSQL database {postgresql_database_identifier} deleted successfully")
    
    return 'SUCCESS', f"PostgreSQL database {postgresql_database_identifier} deleted successfully", ''