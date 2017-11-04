/** This script is to find the count of complaints records where date of registering complaint is same as date when complaint was forwarded to company **/


/**Register piggybank.jar for CSVExcelStorage**/
REGISTER '/usr/local/pig/lib/piggybank.jar';

/** Load data from the input file **/
complaintDetails  = LOAD '/abhilasha/Consumer_Complaints.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER') AS (dateRec:chararray, product:chararray, subProduct:chararray, issue:chararray, subIssue:chararray, complaintNarrative:chararray, companyPublicResponse:chararray, company:chararray, state:chararray, zipCode:chararray, submittedVia:chararray, dateSentToCompany:chararray, companyResponseToConsumer:chararray, timelyResponse:chararray, consumerDisputed:chararray, complaintId:chararray);

/** To get only those records where date of registering complaint is same as date when complaint was forwarded to company  **/
complaintsRequired = FILTER complaintDetails BY dateRec == dateSentToCompany;

/** Grouped complaints by ID to discard duplicates in the data **/
distinctComplaints = GROUP complaintsRequired BY complaintId;

/** To extract complaint ID of all the fields **/
complaintIds = FOREACH distinctComplaints GENERATE group AS complaintID;

/** This grouping is to collect all the complaint ids to be used to get count **/
groupForCount = GROUP complaintIds ALL;

/** To get count of unique complaint ids **/
count = FOREACH groupForCount GENERATE COUNT(complaintIds);

/** To store result in HDFS in a file **/
STORE count INTO '/abhilasha/Project2.2.Task2';
