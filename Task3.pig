/** This script is to find the list of companies with maximum complaints **/


/**Register piggybank.jar for CSVExcelStorage**/
REGISTER '/usr/local/pig/lib/piggybank.jar';

/** Load data from the input file **/
complaintDetails  = LOAD '/abhilasha/Consumer_Complaints.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER') AS (dateRec:chararray, product:chararray, subProduct:chararray, issue:chararray, subIssue:chararray, complaintNarrative:chararray, companyPublicResponse:chararray, company:chararray, state:chararray, zipCode:chararray, submittedVia:chararray, dateSentToCompany:chararray, companyResponseToConsumer:chararray, timelyResponse:chararray, consumerDisputed:chararray, complaintId:chararray);


/** Grouping complaints by company **/
companywiseComplaints = GROUP complaintDetails BY company;

/** Generate count of complaint Ids per group/company **/
companywiseCount = FOREACH companywiseComplaints GENERATE group AS companyName, COUNT(complaintDetails.complaintId) AS countOfComplaints;

/** Order companies in descending order of count **/
orderByCount = ORDER companywiseCount BY countOfComplaints DESC;

/** To get company with highest count **/
companyWithMaxComplaints = LIMIT orderByCount 1;

/** To store result in HDFS in a file **/
STORE companyWithMaxComplaints INTO '/abhilasha/Project2.2.Task3' USING PigStorage('|');



