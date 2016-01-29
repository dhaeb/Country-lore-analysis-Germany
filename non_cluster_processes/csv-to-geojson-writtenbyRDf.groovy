#!/usr/bin/env groovy

import groovy.json.JsonOutput
@Grab('com.xlson.groovycsv:groovycsv:1.1')
import com.xlson.groovycsv.CsvParser
import java.text.ParsePosition
import java.text.SimpleDateFormat

if(args.size() == 1){
    def dateParser = new SimpleDateFormat("yyyyMMdd");
    def csvFile = new File(args[0])
    def csv = csvFile.text
    def data = new CsvParser().parse(csv, separator: ' ', quoteChar: "\"", readFirstLine:true)
    def output = []
    for(splitedLine in data) {
    	def longitude = Double.parseDouble(splitedLine[5])
    	def lat =  Double.parseDouble(splitedLine[6])
    	output.add(
    		[    "type" : "Feature",
    		     "geometry" : ["type" : "Point", "coordinates" : [lat, longitude]],
    		     "properties" : [ 
	    	     	"cAll" : Integer.parseInt(splitedLine[2]),
	    	     	"cOverMean" : Integer.parseInt(splitedLine[1]),
	    	     	"rel" : Double.parseDouble(splitedLine[4]),
	    	     	"Ort" : splitedLine[7],
	    	     	"Bundesland" : splitedLine[8],
	    	     	"Lage" : splitedLine[9],
	    	     	"Hoehe" : splitedLine[10],
	    	     	"von" : dateParser.parse(splitedLine[11], new ParsePosition(0)),
	    	     	"bis" : dateParser.parse(splitedLine[12], new ParsePosition(0))
	    	     ]
	    	] 
    	)	    
    }
    println(JsonOutput.prettyPrint(JsonOutput.toJson(["type" : "FeatureCollection", "features": output])))
}
