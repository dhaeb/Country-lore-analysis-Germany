#!/usr/bin/env groovy
def filename = "kl_total.csv"
def outFile = new File(filename)
def firstLine = 0
if(args.size() >= 1){
	outFile.withPrintWriter { writer ->
		args.findAll{!it.contains("ndf")}.each { curFilePath ->
			def file = new File(curFilePath)		
			println("Try to convert ${curFilePath}")
			def p = ~/(\d{4})(\d{2})(\d{2})/
			file.eachLine(0, { line, lineNumber ->
				if(lineNumber >= firstLine){
					line = line.replaceAll(/\s/, '').trim()
					writer.println(line.replaceAll(p){ all, year, month, day->
						"${year}-${month}-${day}"
					})
				}
			})
			firstLine = 1
		}
	}
} else {
	println("converts to date format and creates a new file")
	println("USAGE files")
}
