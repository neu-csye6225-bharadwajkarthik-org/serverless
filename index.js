const { SecretsManagerClient, GetSecretValueCommand } = require("@aws-sdk/client-secrets-manager");
const nodemailer = require('nodemailer');
const mailGun = require('nodemailer-mailgun-transport');
const axios = require('axios');
const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");
const {v4: uuidv4} = require('uuid'); 
const { Storage } = require('@google-cloud/storage'); 
const fs = require('fs');
const os = require('os');
const path = require('path');

const secretName = process.env['EMAIL_SECRET_NAME']; 
const region = process.env['REGION_AWS']
// AWS Secrets Manager Client
const client = new SecretsManagerClient({ region: region }); 
// Initialize DynamoDB client
const dynamoDBClient = new DynamoDBClient({ region: region }); 

// Async function to retrieve secrets
const getSecret = async () => {
    let secretString;
    try {
        const response = await client.send(new GetSecretValueCommand({
            SecretId: secretName,
            VersionStage: "AWSCURRENT",
        }));
        secretString = response.SecretString;
    } catch (error) {
        // Handle error when retrieving secret
        console.error("Error retrieving secret:", error);
        throw error;
    }
    return secretString;
};

// Lambda function handler
exports.handler = async (event) => {
    try {
        const DYNAMO_TABLE_NAME = process.env['DYNAMO_TABLE_NAME'];
        const BASE64_ENCODED_GCP_SERVICE_ACCOUNT_KEY = process.env['GCP_SERVICE_ACCOUNT_KEY']
        const GCP_SERVICE_ACCOUNT_KEY = Buffer.from(BASE64_ENCODED_GCP_SERVICE_ACCOUNT_KEY, 'base64').toString('utf-8');
        const GCS_BUCKET_NAME = process.env['GCS_BUCKET_NAME'];
        
        const storage = new Storage({
            credentials: JSON.parse(GCP_SERVICE_ACCOUNT_KEY),
        });
        
        const snsMessage = event.Records[0].Sns.Message;
        const parsedMessage = JSON.parse(snsMessage);

        const submissionUrl = parsedMessage.submission_url;
        const email = parsedMessage.email;
        const userId = parsedMessage.userId;
        const assignmentId = parsedMessage.assignmentId;
        const submissionId = parsedMessage.submissionId;
        const SUBMISSION_ERROR = parsedMessage.SUBMISSION_ERROR;
        
        const submissionPath = `assignments/${assignmentId}/users/${userId}/submissions/${submissionId}.zip`;
         // Extract SNS message from the event
        console.log('snsMessage : ', snsMessage)
        console.log('GCS_BUCKET_NAME = ', GCS_BUCKET_NAME)
        
        // Retrieve secret
        const secretValue = await getSecret();
        // Use the secret in your code
        const secretObject = JSON.parse(secretValue || "");
        const domain = secretObject['domain'];
        const apiKey = secretObject['API_KEY'];
        // Create Mailgun transporter
        const auth = {
            auth: {
                domain: domain,
                apiKey: apiKey,
            }
        };
        const transporter = nodemailer.createTransport(mailGun(auth));

        try{
            // Check if max attempts crossed or deadline passed error
            if(SUBMISSION_ERROR){
                throw SUBMISSION_ERROR
            }

            // Check if submission_url starts with http:// or https://
            if (!submissionUrl.toLowerCase().startsWith('http:') && !submissionUrl.toLowerCase().startsWith('https:')) {
                const error = {
                    code: "UNSUPPORTED_PROTOCOL",
                    message: 'Unsupported protocol for submission_url (only http or https allowed)',
                };
                throw error;
            }

            const response = await axios.get(submissionUrl, {// Download the submission file (assumed to be a zip file)
                responseType: 'arraybuffer',
            });

            const contentType = response.headers['content-type'];
            if (!contentType || !contentType.includes('application/zip')) {
                const error = {
                    code: "UNSUPPORTED_FILE_TYPE",
                    message: 'Submission file is not a ZIP file.',
                };
                throw error;
            }

            const zipData = Buffer.from(response.data); // Convert array buffer to buffer
            const tempFilePath = path.join(os.tmpdir(), 'submission.zip'); // Create a temporary file path
            try {
                fs.writeFileSync(tempFilePath, zipData);
                console.log('Temporary file written successfully.');
            
                // Upload the temporary file to Google Cloud Storage
                await storage.bucket(GCS_BUCKET_NAME).upload(tempFilePath, {
                    destination: submissionPath,
                    gzip: true,
                });
                console.log('File uploaded successfully.');
            } catch (err) {                
                console.log('error uploading to gcs bucket : ', err)
                const error = {
                    code: "BUCKET_UPLOAD_ERROR",
                    message: 'GCS Storage service unavailable',
                };
                throw error;
            }
            
            // Send email with attachment if download succeeds
            const mailOptions = {
                from: `noreply@${domain}`,
                to: email,
                subject: 'Submission status',
                text: `Submission successfully downloaded and stored in gcs bucket with name : ${GCS_BUCKET_NAME}, at path : ${submissionPath}.`,
            };

            await transporter.sendMail(mailOptions);
            console.log("Email sent!");
             // Prepare the item to be inserted into DynamoDB
            const dynamoDBParams = {
                TableName: DYNAMO_TABLE_NAME, // Replace with your DynamoDB table name
                Item: {
                    id: { S: uuidv4() }, // Define a unique identifier for each record
                    email: { S: email }, // Store the email address
                    status: { S: 'Success'}, // Store success/failure status
                    body: {S: `Submission successfully downloaded and stored in gcs bucket with name : ${GCS_BUCKET_NAME}, at path : ${submissionPath}, and status mailed to ${email}`},
                    timestamp: { S: `${new Date().toISOString()}` }, // Store a timestamp for sorting or reference
                    // Add more attributes or modify as needed based on your requirements
                },
            };

            // Define the PutItem command to insert the record
            const putItemCommand = new PutItemCommand(dynamoDBParams);

            // Insert record into DynamoDB
            try{
                await dynamoDBClient.send(putItemCommand);
            }catch(error){
                console.log('Insert into DB failed!')
                return {
                    statusCode: 503,
                    body: JSON.stringify('Insert into DB failed!'),
                };
            }
            
            // Return success response
            return {
                statusCode: 200,
                body: JSON.stringify('Download succeeded!'),
            };
        }catch(error){
            let mailOptions;
            const dynamoDBParams = {
                TableName: DYNAMO_TABLE_NAME, // Replace with your DynamoDB table name
                Item: {
                    id: { S: uuidv4() }, // Define a unique identifier for each record
                    email: { S: email }, // Store the email address
                    status: { S: 'Failure'}, // Store success/failure status
                    body: {S: ''},
                    timestamp: { S: `${new Date().toISOString()}` }, // Store a timestamp for sorting or reference
                    // Add more attributes or modify as needed based on your requirements
                },
            };
            if(error.code === "UNSUPPORTED_PROTOCOL"){ //error.code === 'ERR_BAD_REQUEST'
                mailOptions = {
                    from: `noreply@${domain}`,
                    to: email,
                    subject: 'Submission status',
                    text: 'Submission download was unsuccessful due to unsupported submission_url protocol (must be http or https)',
                };
                dynamoDBParams.Item.body.S = `Submission download was unsuccessful due to unsupported submission_url protocol (must be http or https), and failure mail sent to ${email}`
            }else if(error.code === 400){
                mailOptions = {
                    from: `noreply@${domain}`,
                    to: email,
                    subject: 'Submission status',
                    text: `Submission download was unsuccessful as ${error.msg}`,
                };
                dynamoDBParams.Item.body.S = `Submission download was unsuccessful as ${error.msg}, and failure mail sent to ${email}`
            }else if(error.code === 'UNSUPPORTED_FILE_TYPE'){
                mailOptions = {
                    from: `noreply@${domain}`,
                    to: email,
                    subject: 'Submission status',
                    text: 'Submission download was unsuccessful as file present in submission_url is not a ZIP file',
                };
                dynamoDBParams.Item.body.S = `Submission download was unsuccessful as file present in submission_url is not a ZIP file, and failure mail sent to ${email}`
            }else if(error.code === 'ENOTFOUND' || error.code === 'ERR_BAD_REQUEST'){
                mailOptions = {
                    from: `noreply@${domain}`,
                    to: email,
                    subject: 'Submission status',
                    text: 'Submission download was unsuccessful as no resource found in given submission_url',
                };
                dynamoDBParams.Item.body.S = `Submission download was unsuccessful as no resource found in given submission_url, and failure mail sent to ${email}`
            }else if(error.code === 'ECONNREFUSED'){
                mailOptions = {
                    from: `noreply@${domain}`,
                    to: email,
                    subject: 'Submission status',
                    text: 'Submission download was unsuccessful due to malformed submission_url format (invalid URL structure)',
                };
                dynamoDBParams.Item.body.S = `Submission download was unsuccessful due to malformed submission_url format (invalid link structure), and failure mail sent to ${email}`
            }else if(error.code === 'BUCKET_UPLOAD_ERROR'){
                mailOptions = {
                    from: `noreply@${domain}`,
                    to: email,
                    subject: 'Submission status',
                    text: `Submission download was successful, but upload to storage failed due to ${error.message}`,
                };
                dynamoDBParams.Item.body.S = `Submission download was successful, but upload to storage failed due to ${error.message}, and failure mail sent to ${email}`
            }else if(error.code === 'ETIMEDOUT'){
                mailOptions = {
                    from: `noreply@${domain}`,
                    to: email,
                    subject: 'Submission status',
                    text: 'Submission download was unsuccessful due to connection timeout (file too large)',
                };
                dynamoDBParams.Item.body.S = `Submission download was unsuccessful due to connection timeout (file too large), and failure mail sent to ${email}`
            }else{
                console.log('Error : ', error)
                mailOptions = {
                    from: `noreply@${domain}`,
                    to: email,
                    subject: 'Submission status',
                    text: 'Submission download was unsuccessful due to unexpected problems',
                };
                dynamoDBParams.Item.body.S = `Submission unsuccessful. Download failed due to due to unexpected problems, and failure mail sent to ${email}`
            }
        
            await transporter.sendMail(mailOptions)

            // Define the PutItem command to insert the record
            const putItemCommand = new PutItemCommand(dynamoDBParams);

            // Insert record into DynamoDB
            try{
                await dynamoDBClient.send(putItemCommand);
            }catch(error){
                return {
                    statusCode: 503,
                    body: JSON.stringify('Insert into DB failed!'),
                };
            }
            // Return error response
            return {
                statusCode: 503,
                body: JSON.stringify("Error sending email"),
            };
        }
    } catch (error) {
        console.error("Error :", error);
        return {
            statusCode: 503,
            body: JSON.stringify("Error sending email"),
        };
    }
};
