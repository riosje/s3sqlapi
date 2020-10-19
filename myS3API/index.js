const AWS = require("aws-sdk");
const s3 = new AWS.S3();

exports.handler = async (event, context) => {
  let statusCode = "200";
  const headers = {
    "Content-Type": "application/json",
  };

  //Incoming parameters validation
  if (event.queryStringParameters == null || event.queryStringParameters == undefined){
	  statusCode = "500";
	  var body = '{"error": "Any parameter was found"}';
	  return {
		body,
		statusCode,
		headers,
		isBase64Encoded: false,
	  };
  }

  var s3SelectQuery = 'select * from s3object s where ';
  var reqMonth = event.queryStringParameters.month;
  var reqYear = event.queryStringParameters.year;
  var reqDate = event.queryStringParameters.date;

  // Check if exist a date paramer or month and year parameters
  if (reqDate != null && reqDate != undefined){
	  s3SelectQuery += 's."Date" LIKE \'' + reqDate + '%\' ';

  }else if(reqMonth != null && reqMonth != undefined && reqYear != null && reqYear != undefined){
		s3SelectQuery += 's."Date" LIKE \'' + reqYear + '-' + reqMonth + '%\'';
		console.log(s3SelectQuery)
  }else{
	  statusCode = "500";
	  body = '{"error": "Valid parameters: date, month, year"}';
	  return {
		body,
		statusCode,
		headers,
		isBase64Encoded: false,
	  };
  }

  const SQLQuery = s3SelectQuery;
  const s3params = {
    Bucket: "jefferson.private.bucket",
    Expression: SQLQuery,
    ExpressionType: "SQL",
    InputSerialization: {
      CSV: {
        AllowQuotedRecordDelimiter: true,
        FieldDelimiter: ",",
        FileHeaderInfo: "USE",
      },
    },
    Key: "CSEI_COLCAP_IQY.csv",
    OutputSerialization: {
      JSON: {
        RecordDelimiter: ",",
      },
    },
  };

  body = await SQLSelectFunction(s3params);
  return {
    body,
    statusCode,
    headers,
    isBase64Encoded: false,
  };
};

const SQLSelectFunction = async function (params) {
  return new Promise((resolve, err) => {
    s3.selectObjectContent(params, function (err, data) {
      if (err) {
        // handle error
        return console.error(err);
      }
      var eventStream = data.Payload;
      const records = [];
      eventStream.on("data", function (event) {
        // Check the top-level field to determine which event this is.
        if (event.Records) {
          records.push(event.Records.Payload);
        } else if (event.Stats) {
          // handle Stats event
        } else if (event.Progress) {
          // handle Progress event
        } else if (event.Cont) {
          // handle Cont event
        } else if (event.End) {
          // handle End event
        }
      });
      eventStream.on("error", function (err) {
        console.log(err);
      });
      eventStream.on("end", function () {
        let response = Buffer.concat(records).toString("utf8");
        response = response.replace(/\,$/, "");
        response = `[${response}]`;
        response = JSON.parse(response);
        response = JSON.stringify(response);
        resolve(response);
      });
    });
  });
};
