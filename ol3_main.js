/*
Dependencies: ol3_script.js, ol3_dialog.js, jquery
*/

var test;

$(function(){
    showDialog(function(content){
        //run(JSON.parse(content));
        run(content);
    });
});
