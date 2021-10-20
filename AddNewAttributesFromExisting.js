/**
 * USE: This function will scan a table and add new attributes to the table based on existing attributes
 * If the new attribute already exists, it will not be added
 *
 * Expectation: the specific attributes to be extracted are deeply nested in the data returned from the table
 */

var AWS = require("aws-sdk");

AWS.config.update({
  region: "us-west-2", // Update as needed
});

const table = "TABLE_NAME_GOES_HERE";

var docClient = new AWS.DynamoDB.DocumentClient();

// To be used in the scan function to retrieve all the items in the table
const params = {
  TableName: table,
  ProjectionExpression: "#tid, attribute_1, attribute_2",
  ExpressionAttributeNames: {
    "#tid": "table_id",
  },
};

console.log("Scanning the table.");
docClient.scan(params, onScan);

const onScan = (err, data) => {
  if (err) {
    console.error(
      "Unable to scan the table. Error JSON:",
      JSON.stringify(err, null, 2)
    );
  } else {
    console.log("Scan succeeded.");

    // Separate the necessary attributes from the returned data
    const parsedDataValues = parseInfoList(data);

    // Add the new attributes to the selected table
    addNewAttributes(parsedDataValues);

    // Continue scanning if we have more items, because scan can retrieve a maximum of 1MB of data
    if (typeof data.LastEvaluatedKey != "undefined") {
      console.log("Scanning for more...");
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      docClient.scan(params, onScan);
    }
  }
};

const parseInfoList = (data) => {
  const parsedData = data.Items.map((dataItem) => {
    const { parent_attribute } = dataItem;

    // Here, table_id and created_at act as the primary key and sort key for the table,
    // and will be used to differentiate between items
    const res = {
      table_id: dataItem.table_id,
      created_at: dataItem.created_at,
      new_attribute: parent_attribute.new_attribute,
    };

    return res;
  });

  return parsedData;
};

// Create the new items by updating each row by table identifier
const addNewAttributes = (values) => {
  values.forEach((valueItem) => {
    const params = {
      TableName: table,
      Key: {
        table_id: valueItem.table_id,
        created_at: valueItem.created_at,
      },
      ConditionExpression: "attribute_not_exists(new_attribute)",
      UpdateExpression: "SET new_attribute = :n",
      ExpressionAttributeValues: {
        ":n": valueItem.new_attribute,
      },
      ReturnValues: "UPDATED_NEW",
    };

    // Update each item in the table with the new attributes
    docClient.update(params, function (err, data) {
      if (err) {
        console.error(
          "Unable to update item. Error JSON:",
          JSON.stringify(err, null, 2)
        );
      } else {
        console.log("UpdateItem succeeded:", JSON.stringify(data, null, 2));
      }
    });
  });
};
