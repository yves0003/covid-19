
//Authentification

function getAuthType() {
  return {
    type: "NONE"
  };
}

//Configuration

function getConfig(request) {
  var config = {
    configParams: [
      {
        type: "INFO",
        name: "connect",
        text: "This connector does not require any configuration. Click CONNECT at the top right to get started."
      }
    ],
    dateRangeRequired: false
  };
  return config;
};

//Création des schema pour BigQuery

var table = {
  tableReference : {
    projectId: 'xxxx-xxxxxx',
    datasetId: 'xxxxx',
    tableId: 'xxx'
  },
  schema : {
    fields: [
      {
        name: 'code',
        type: 'STRING'
      },
      {
        name: 'nom',
        type: 'STRING'
      },
      {
        name: 'date_extract',
        type: 'STRING'
      },
      {
        name: 'hospitalises',
        type: 'INTEGER'
      },
      {
        name: 'reanimation',
        type: 'INTEGER'
      },
      {
        name: 'nouvellesHospitalisations',
        type: 'INTEGER'
      },
      {
        name: 'nouvellesReanimations',
        type: 'INTEGER'
      },
      {
        name: 'deces',
        type: 'INTEGER'
      },
      {
        name: 'decesEhpad',
        type: 'INTEGER'
      },
      {
        name: 'gueris',
        type: 'INTEGER'
      },
      {
        name: 'sourceType',
        type: 'STRING'
      },
      {
        name: 'source',
        type: 'STRING'
      }
    ]
  }
}

//Ecraser les anciennes données et les remplacer
var writeDispositionSetting = 'WRITE_TRUNCATE';

//Listes des départements + France

let dep = ['Ain',
'Aisne',
'Allier',
'Alpes-de-Haute-Provence',
'Hautes-Alpes',
'Alpes-Maritimes',
'Ardèche',
'Ardennes',
'Ariège',
'Aube',
'Aude',
'Aveyron',
'Bouches-du-Rhône',
'Calvados',
'Cantal',
'Charente',
'Charente-Maritime',
'Cher',
'Corrèze',
'Corse-du-Sud',
'Haute-Corse',
"Côte-d'Or",
"Côtes-d'Armor",
'Creuse',
'Dordogne',
'Doubs',
'Drôme',
'Eure',
'Eure-et-Loir',
'Finistère',
'Gard',
'Haute-Garonne',
'Gers',
'Gironde',
'Hérault',
'Ille-et-Vilaine',
'Indre',
'Indre-et-Loire',
'Isère',
'Jura',
'Landes',
'Loir-et-Cher',
'Loire',
'Haute-Loire',
'Loire-Atlantique',
'Loiret',
'Lot',
'Lot-et-Garonne',
'Lozère',
'Maine-et-Loire',
'Manche',
'Marne',
'Haute-Marne',
'Mayenne',
'Meurthe-et-Moselle',
'Meuse',
'Morbihan',
'Moselle',
'Nièvre',
'Nord',
'Oise',
'Orne',
'Pas-de-Calais',
'Puy-de-Dôme',
'Pyrénées-Atlantiques',
'Hautes-Pyrénées',
'Pyrénées-Orientales',
'Bas-Rhin',
'Haut-Rhin',
'Rhône',
'Haute-Saône',
'Saône-et-Loire',
'Sarthe',
'Savoie',
'Haute-Savoie',
'Paris',
'Seine-Maritime',
'Seine-et-Marne',
'Yvelines',
'Deux-Sèvres',
'Somme',
'Tarn',
'Tarn-et-Garonne',
'Var',
'Vaucluse',
'Vendée',
'Vienne',
'Haute-Vienne',
'Vosges',
'Yonne',
'Territoire de Belfort',
'Essonne',
'Hauts-de-Seine',
'Seine-Saint-Denis',
'Saint-Barthélemy',
'Saint-Martin',
'Nouvelle-Calédonie',
'Val-de-Marne',
"Val-d'Oise",
'Guadeloupe',
'Martinique',
'Guyane',
'La Réunion',
'Mayotte',
'France'];


function prepareSchema(request) {
  // Prepare the schema for the fields requested.
  var dataSchema = [];
  var fixedSchema = getSchema().schema;
  request.fields.forEach(function(field) {
    for (var i = 0; i < fixedSchema.length; i++) {
      if (fixedSchema[i].name == field.name) {
        dataSchema.push(fixedSchema[i]);
        break;
      }
    }
  });
  console.log('prepare schema');
  return dataSchema;
}


//Récupération des données historiques

 async function load_data () {
   console.log('load data');
    var all_data=[]; 
	const promises = await dep.map(async departement => {
		const dataDep = await UrlFetchApp.fetch("https://coronavirusapi-france.now.sh/AllDataByDepartement?Departement="+departement);
        const response = await JSON.parse(dataDep.getContentText());
		await all_data.push(...response['allDataByDepartement']);
		return;
	 });
	const allDep = await Promise.all(promises);
    await console.log(all_data);
    return all_data;
  }
  
//Sauvegarder les données hsitoriques

async function getData_and_save() {
    console.log('get data');
    var plays= await load_data();
    var lignesCSV = await buildTabularData(plays, table.schema.fields);
    Logger.log(lignesCSV);
    var blob = Utilities.newBlob(lignesCSV, "text/csv");
    var data = blob.setContentType('application/octet-stream');
  
    //Créer une nouvelle table si elle n'existe pas
    try{BigQuery.Tables.get(table.tableReference.projectId, table.tableReference.datasetId, table.tableReference.tableId)} 
    catch (error){
      function createDataSet() {
       var dataSet = BigQuery.newDataset();
       dataSet.id = table.tableReference.datasetId;
       dataSet.datasetReference = BigQuery.newDatasetReference();
       dataSet.datasetReference.projectId = table.tableReference.projectId;
       dataSet.datasetReference.datasetId = table.tableReference.datasetId;
       dataSet = BigQuery.Datasets.insert(dataSet, table.tableReference.projectId);
       Logger.log('Data set with ID = %s.', dataSet.id);
      }
      createDataSet();
      table = BigQuery.Tables.insert(table, table.tableReference.projectId, table.tableReference.datasetId);
    }
  
  // La requete
  var job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: table.tableReference.projectId,
          datasetId: table.tableReference.datasetId,
          tableId: table.tableReference.tableId
        },
        writeDisposition: writeDispositionSetting
      }
    }
  };
  
  // Envoyer les données par une la requete à bigQuery pour qu'il execute le script
  
  var runQuery = BigQuery.Jobs.insert(job, table.tableReference.projectId, data);
  Logger.log(runQuery.status);
  var jobId = runQuery.jobReference.jobId
  Logger.log('jobId: ' + jobId);
  var status = BigQuery.Jobs.get(table.tableReference.projectId, jobId);
  
  // Attendre la fin de la requête avant de poursuivre
  
  while (status.status.state === 'RUNNING') {
    Utilities.sleep(500);
    status = BigQuery.Jobs.get(table.tableReference.projectId, jobId);
    Logger.log('Status: ' + status);
  }
  Logger.log('TERMINE!!!!');
  
  
}



function buildTabularData(plays, dataSchema) {
  // Prepare the tabular data.
  var data = [];

  plays.forEach(function(play){
    var values = [];
    var playExtract = new Date(play.date.replace(/[^0-9]/g, "-"));
    // Google expects YYYYMMDD format
    var date_extract = playExtract.toISOString().slice(0, 10).replace(/-/g, "");
    //var date_extract = play.date;
    // Provide values in the order defined by the schema - on récupère les valeurs du schema.
    dataSchema.forEach(function(field) {
        switch (field.name) {
          case 'code':
            values.push(play? (play.code.replace(',',"")): "");
            break;
          case 'nom':
            values.push(play? (play.nom.replace(',',"")): "");
            break;
          case 'date_extract':
            values.push(date_extract.replace(',',""));
            break;
          case 'hospitalises':
            values.push(play.hospitalises ? play.hospitalises: 0);
            break;
          case 'reanimation':
            values.push(play.reanimation ? play.reanimation : 0);
            break;
          case 'nouvellesHospitalisations':
            values.push(play.nouvellesHospitalisations ? play.nouvellesHospitalisations : 0);
            break;
          case 'nouvellesReanimations':
            values.push(play.nouvellesReanimations ? play.nouvellesReanimations : 0);
            break;
          case 'deces':
            values.push(play.deces ? play.deces : 0);
            break;
          case 'decesEhpad':
            values.push(play.decesEhpad ? play.decesEhpad : 0);
            break;
          case 'gueris':
            values.push(play.gueris ? play.gueris : 0);
            break;
          case 'sourceType':
            values.push(play.sourceType ? play.sourceType.replace(',',"") : "");
            break;
          case 'source':
            values.push(play.source ? (play.source.nom ? play.source.nom.replace(',',"") : "") : "");
            break;
          default:
            values.push('');
        }
    });
    
    data.push(
      values
    );

  });
  
  //console.log(data.join('\n'));
    return data.join('\n');
}


function isAdminUser() {
  return true;
}

Date.prototype.yyyymmdd = function() {
  this.setDate(this.getDate() - 1);
  var mm = this.getMonth() + 1; // getMonth() is zero-based
  var dd = this.getDate();
  
  return [this.getFullYear(),
          (mm>9 ? '' : '0') + mm,
          (dd>9 ? '' : '0') + dd
         ].join('-');
};

function load_data_day() {
  
  
  

  console.log("https://coronavirusapi-france.now.sh/AllDataByDate?date="+ date);
}

 async function load_data_yesterday () {
   console.log('load data yesterday');
   var date = new Date().yyyymmdd();
   var all_data= await UrlFetchApp.fetch("https://coronavirusapi-france.now.sh/AllDataByDate?date="+date);
   const response = await JSON.parse(all_data.getContentText());
   await console.log(response.allFranceDataByDate);
   return response.allFranceDataByDate;
  }

async function getData_yesterday_and_save() {
    console.log('get data');
    var plays= await load_data_yesterday();
    var lignesCSV = await buildTabularData(plays, table.schema.fields);
    Logger.log(lignesCSV);
    var blob = Utilities.newBlob(lignesCSV, "text/csv");
    var data = blob.setContentType('application/octet-stream');
  
  // La requete
  var job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: table.tableReference.projectId,
          datasetId: table.tableReference.datasetId,
          tableId: table.tableReference.tableId
        },
        writeDisposition: 'WRITE_APPEND'
      }
    }
  };
  
  // Envoyer les données par une la requete à bigQuery pour qu'il execute le script
  
  var runQuery = BigQuery.Jobs.insert(job, table.tableReference.projectId, data);
  Logger.log(runQuery.status);
  var jobId = runQuery.jobReference.jobId
  Logger.log('jobId: ' + jobId);
  var status = BigQuery.Jobs.get(table.tableReference.projectId, jobId);
  
  // Attendre la fin de la requête avant de poursuivre
  
  while (status.status.state === 'RUNNING') {
    Utilities.sleep(500);
    status = BigQuery.Jobs.get(table.tableReference.projectId, jobId);
    Logger.log('Status: ' + status);
  }
  Logger.log('TERMINE!!!!');
  
  
}

