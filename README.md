# avro2Json
avro to json in python


Lambda is listening to a s3 bucket. 
Lambda should be configured to listen for .avro files on a source s3 bucket.
Source bucket should be named {account}.{profile}.braze
Create bucket for processed files, which should be named by appending -processed to the source bucket name.
As files are processed they are moved from the source bucket to the processed bucket.


Possible Updates:
Should probably not only copy the file to a new bucket
but listen for success on sending data to Tealium and
if success append row to success bucket, else append
row to failure bucket for reprocessing.

Braze may require pathing structure as there are different 'currents'
aka different actions captured such as open, view, etc... thus, there may need to be updates to handle this.
