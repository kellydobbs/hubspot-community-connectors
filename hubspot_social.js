var hubspotApiKey = '';
var count = 500;

var cc = DataStudioApp.createCommunityConnector();



// Auth
function getAuthType() {
  var AuthTypes = cc.AuthType;
  return cc
    .newAuthTypeResponse()
    .setAuthType(AuthTypes.KEY)
    .setHelpUrl('https://developers.google.com/datastudio/connector/auth#key_2')
    .build();
}


function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.key');
}

function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty('dscc.key');
  return checkForValidKey(key);
}

function setCredentials(request) {
  var key = request.key
  var validKey = checkForValidKey(key);
  if (!validKey) {
    return {
      errorCode: 'INVALID_CREDENTIALS'
    };
  }
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('dscc.key', key);
  return {
    errorCode: 'NONE'
  };
}



// Config
function getConfig() {
  var config = cc.getConfig();
  return config.build();
}



// Get and Format Data
function getFields(request) {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

var fieldsArray = [
  {id:'triggerAt', name: 'Publish Date', type: types.YEAR_MONTH_DAY},
  {id:'broadcastGuid', name: 'ID', type: types.TEXT},
  {id:'channelKey', name: 'Social Channel', type: types.TEXT},
  {id:'clicks', name: 'Clicks', type: types.NUMBER},
  {id:'messageText', name: 'Posts', type: types.TEXT}, // content of the post
  {id:'title', name: 'Link', type: types.TEXT}, // link in the post - content.title
  {id:'campaignGuid', name: 'Campaign ID', type: types.TEXT}, // HubSpot campaign
];

fieldsArray.forEach(function(each){
  fields.newDimension()
    .setId(each.id)
    .setName(each.name)
    .setType(each.type)
})
   return fields;
}


function getSchema(request) {
  var fields = getFields(request).build();
  return { schema: fields };
}


function convertTimestap(publishDate) {
  var date = new Date(publishDate);
  var formattedDate = Utilities.formatDate(date, "CST", "yyyyMMdd");
  return formattedDate;
}


function formatData(apiResponse, requestedFields) { 
 var socialArray = apiResponse; // apiResponse is an array of objects
 var rows = [] 


for (let i = 0; i < socialArray.length; i++) {
 var row = requestedFields.asArray().map(function(field) { 
      switch (field.getId()) {
       case 'triggerAt':
         return convertTimestap(socialArray[i].triggerAt);
        case 'broadcastGuid':
         return socialArray[i].broadcastGuid;
        case 'channelKey':
         return socialArray[i].channelKey;
        case 'clicks':
          return socialArray[i].clicks;
       case 'messageText':
           return socialArray[i].messageText; 
       case 'title':
           return socialArray[i].content.title; 
       case 'campaignGuid':
           return socialArray[i].campaignGuid;    
       default :
            return '';
        }
    });
    rows.push({ values: row })
}
    return rows;
}

function fetchDataFromApi(){
 var baseURL = 'https://api.hubapi.com/broadcast/v1/broadcasts?hapikey='+ hubspotApiKey  + '&status=success'+'&count=' + count;
 var response = UrlFetchApp.fetch(baseURL);
 return JSON.parse(response.getContentText());
}


function getData(request) {
    var requestedFields = getFields().forIds(
      request.fields.map(function(field) {
        return field.name;
    })
);

try {
    var apiResponse = fetchDataFromApi(request);
    var data = formatData(apiResponse, requestedFields);
  } catch(e) {
      cc.newUserError()
        .setDebugText('Error fetching data from API. Exception details: ' + e)
        .setText('Error fetching data from API.')
        .throwException();
  }
   return {
      schema: requestedFields.build(),
      rows: data
    };
}



function checkForValidKey(key) {
  var token = key;
  var today = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  var baseURL = 'https://api.hubapi.com/marketing-emails/v1/emails/with-statistics?hapikey='+ 
    hubspotApiKey + '&publish_date__lte=' + today
  var response = UrlFetchApp.fetch(baseURL);
  if (response.getResponseCode() == 200) {
    return true;
  } else {
    return false;
  }
}

function isAdminUser() {
  return true;
}