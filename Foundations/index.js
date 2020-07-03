var AWS = require("aws-sdk");
AWS.config.update({region:'us-east-1'});

const ec2Object = new AWS.EC2();
var params = {
  Filters: [
    {
      Name: 'instance-state-name',
      Values: [
        'terminated',
      ]
    },
  ]
};
ec2Object.describeInstances(params, function(err, data) {
  if (err) console.log(err, err.stack); // an error occurred
  else     console.log(data);           // successful response
});