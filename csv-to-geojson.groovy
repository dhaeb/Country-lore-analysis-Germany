#!/usr/bin/env groovy

import groovy.json.JsonOutput
@Grab('com.xlson.groovycsv:groovycsv:1.1')
import com.xlson.groovycsv.CsvParser

if(args.size() == 1){
    def csvFile = new File(args[0])
    def csv = csvFile.text
    def data = new CsvParser().parse(csv, separator: ',', quoteChar: "\"", readFirstLine:true)
    def output = []
    for(splitedLine in data) {
    	def longitude = Double.parseDouble(splitedLine[3])
    	def lat =  Double.parseDouble(splitedLine[4])
    	output.add(
    		[    "type" : "Feature",
    		     "geometry" : ["type" : "Point", "coordinates" : [lat, longitude]],
    		     "properties" : [ 
	    	     	"cAll" : Integer.parseInt(splitedLine[0]),
	    	     	"cOverMean" : Integer.parseInt(splitedLine[1]),
	    	     	"rel" : Double.parseDouble(splitedLine[2]),
	    	     	"Ort" : splitedLine[5],
	    	     	"Bundesland" : splitedLine[6],
	    	     	"Lage" : splitedLine[7]
	    	     ]
	    	] 
    	)	    
    }
    println(JsonOutput.prettyPrint(JsonOutput.toJson(["type" : "FeatureCollection", "features": output])))
}
