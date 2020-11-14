var hubspotApiKey = '';

var cc = DataStudioApp.createCommunityConnector();

//Auth
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


//Config
function getConfig() {
  var config = cc.getConfig();
  return config.build();
}

//Get & Format Data
function getFields(request) {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

var fieldsArray = [
  {id:'publishDate', name: 'Publish Date', type: types.YEAR_MONTH_DAY},
  {id:'id', name: 'ID', type: types.NUMBER},
  {id:'name', name: 'Email Name', type: types.TEXT},
  {id:'campaignName', name: 'Campaign', type: types.TEXT},
  {id:'campaign', name: 'Campaign ID', type: types.TEXT},
  {id:'state', name: 'Email Type', type: types.TEXT},
  {id:'open', name: 'Opens', type: types.NUMBER},
  {id:'sent', name: 'Sent', type: types.NUMBER},
  {id:'click', name: 'Clicks', type: types.NUMBER},
  {id:'delivered', name: 'Delivered', type: types.NUMBER},
  {id:'clickthroughratio', name: 'CTR', type: types.NUMBER},
  {id:'clickratio', name: 'Click Rate', type: types.NUMBER},
  {id:'openratio', name: 'Open Rate', type: types.NUMBER}
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
 var objects = apiResponse.objects; 
 var emails = []; // array of email objects
 var rows = [] 

for (let i = 0; i < objects.length; i++) {
  emails.push(apiResponse.objects[i]);
}

for (let i = 0; i < objects.length; i++) {
  var row = requestedFields.asArray().map(function(field) { 
      switch (field.getId()) {
       case 'publishDate':
         return convertTimestap(emails[i].publishDate); // returns null; fix data format in function
        case 'id':
         return emails[i].id;
        case 'name':
         return emails[i].name;
        case 'campaignName':
          return emails[i].campaignName;
        case 'campaign':
          return emails[i].campaign;
        case 'state':
          return emails[i].state;
       case 'open':
           return emails[i].stats.counters.open;
       case 'sent':
          return emails[i].stats.counters.sent;            
       case 'click':
           return emails[i].stats.counters.click; 
       case 'delivered':
           return emails[i].stats.counters.delivered; 
       case 'clickthroughratio':
           return emails[i].stats.ratios.clickthroughratio; 
       case 'clickratio':
           return emails[i].stats.ratios.clickratio;            
       case 'openratio':
           return emails[i].stats.ratios.openratio;  
       default :
            return '';
        }
    });
    rows.push({ values: row })
}
    return rows;
}

function fetchDataFromApi(){
 var baseURL = 'https://api.hubapi.com/marketing-emails/v1/emails/with-statistics?hapikey='+ hubspotApiKey + '&limit=46';
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