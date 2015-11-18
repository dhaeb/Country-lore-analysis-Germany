#!/usr/bin/env groovy
if(args.size() >= 1){
	args.findAll{!it.contains("ndf")}.each { curFilePath ->
		def file = new File(curFilePath)		
		def outFile = new File(curFilePath.substring(0, curFilePath.size() - 4) + "_ndf.txt")
		println("Try to convert ${curFilePath}")
		def p = ~/(\d{4})(\d{2})(\d{2})/
		outFile.withPrintWriter { writer -> 
		  	file.eachLine { line ->
		  		if(!line.isEmpty()){
					line = line.replaceAll(/\s/, '')
					writer.println(line.replaceAll(p){ all, year, month, day->
						"${year}-${month}-${day}"
					})
				}
			}
		}
	}
} else {
	println("converts to date format and creates a new file")
	println("USAGE files")
}
