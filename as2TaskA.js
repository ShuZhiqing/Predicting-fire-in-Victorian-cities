// Q1
// Import the data (Fire data-Part 1 and Weather data-Part 1) into two different collections in MongoDB.
// mongoimport --db fit5148_db --collection climate --type csv --headerline --file /Users/yolanda/Downloads/ClimateData-Part1.csv
// mongoimport --db fit5148_db --collection fire --type csv --headerline --file /Users/yolanda/Downloads/FireData-Part1.csv

// display all databases
show dbs
// switch to the database
use fit5148_db
// show collections
show collections

// Q2
// Find climate data on 15th December 2017.
db.climate.find({ 'Date': '2017-12-15'} , {'_id': 0}).pretty()

// Q3
// Find the latitude, longitude and confidence when the surface temperature (°C) was between 65 °C and 100 °C.
db.fire.find( {'Surface Temperature (Celcius)' : { $gte:65, $lte:100} },{'Latitude': 1,'Longitude': 1,'Confidence': 1,'Surface Temperature (Celcius)': 1,'_id':0}).pretty()

// Q4
// Find ​surface temperature (°C), air temperature (°C), relative humidity​ and ​maximum wind speed ​on 15th and 16th of December 2017.
db.climate.aggregate([{
    $match: { "Date": { $in: [ "2017-12-15", "2017-12-16" ] } }
    },
    {
        $lookup:
        {
            from: "fire",
            localField : "Date",
            foreignField : "Date",
            as : "FireData"
        }
    },
    {
        $unwind: "$FireData"
    },
    {
        $replaceRoot: { newRoot: { $mergeObjects: [ "$FireData", "$$ROOT" ] } }
    },
    {
        $project: 
        { 
            "_id": 0,
            "Surface Temperature (Celcius)" : 1, 
            "Air Temperature(Celcius)": 1,
            "Relative Humidity": 1,
            "Date": 1,
            "Max Wind Speed": 1
        }
    }
]).pretty()

// Q5 
// Find ​datetime, air temperature (°C), surface temperature (°C)​ and ​confidence ​when the confidence ​is between 80 and 100

db.fire.aggregate([
    {
        $match: { 'Confidence' : { $gte:80, $lte:100} }
    },
    {
        $lookup:
        {
            from: "climate",
            localField : "Date",
            foreignField : "Date",
            as : "climate"
        }
    },
    {
        $unwind: "$climate"
    },
    {
        $replaceRoot: { newRoot: { $mergeObjects: [ "$climate", "$$ROOT" ] } }
    },
    {
        $project: 
        { 
            "_id": 0,
            "Datetime" : 1, 
            "Air Temperature(Celcius)": 1,
            "Surface Temperature (Celcius)": 1,
            "Confidence": 1
        }
    }
]).pretty()

// Q6
// Find top 10 records with highest surface temperature (°C).

db.fire.find({},{"_id":0}).sort({"Surface Temperature (Celcius)":-1}).limit(10).pretty();

// Q7
// Find the number of fire in each day. You are required to only display total number of fire and the date in the output

db.fire.aggregate(
[
    {
        $group : 
        {
            _id : "$Date",
            "Firecount" : {$sum : 1}
        }
    },
    {
        $sort :
        {
            '_id': 1
        }
    },
    { 
        $project: 
        {  
            _id: 0,
            "Date": "$_id",
            Firecount: 1
        }
    }

])


// Q8
// Find the average surface temperature (°C) for each day. You are required to only display average surface temperature (°C) ​and the date in the output.
db.fire.aggregate(
[
    {
        $group : 
        {
            _id : "$Date",
            "Avg temperature" : {$avg : "$Surface Temperature (Celcius)"}
        }
    },
    {
        $sort :
        {
            '_id': 1
        }
    },
    { 
        $project: 
        {  
            _id: 0,
            "Date" : "$_id",
            "Avg temperature" : 1
        }
    }
])
