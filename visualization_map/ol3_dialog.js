/*
Dependencies: jquery, html5:fileAPI, html5:dialog
*/

var readFile = function(fileInput, resultCallback) {
    //Retrieve all the files from the FileList object
    var file = fileInput.files[0];
    if (file) {
        var reader = new FileReader();
        reader.onload = (function (file) {
            return function (e) {
                var contents = e.target.result;
                resultCallback(contents);
            };
        })(file);
        reader.readAsText(file);
    } else {
        alert("Failed to load file");
    }
}

var notEmptyInput = function(fileInput){
    return fileInput.files.length != 0;
}

var getInputElement = function(){
    return $("<input type='file' id='fileinput' accept='.json'/>");
}

var getCloseButton = function(dialog, fileInput, resultCallback){
    return $("<button>")
        .text("OK")
        .bind("click", function(){ 
            if(notEmptyInput(fileInput)){
                readFile(fileInput, resultCallback); 
                dialog.close(); 
                dialog.parentNode.removeChild(dialog);
            }
        }
    );
}

var showDialog = function(resultCallback){
    var dialog = $("<dialog><dialog/>");
    var fileInputElement = getInputElement();
    dialog.append(fileInputElement)
        .append(getCloseButton(dialog[0], fileInputElement[0], resultCallback))
        .appendTo(document.body)[0]
        .showModal();
}

