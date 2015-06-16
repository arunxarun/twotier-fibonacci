// main.js


        
$(document).ready(function() {
	
	var inProcessTable = $('#inprocess').DataTable( {
        "order": [[ 2, "desc" ]]
    });
	
	var completeTable = $('#complete').DataTable( {
        "order": [[ 3, "desc" ]]
    });

	function loadTable() {
		$.get('/inprocess',function(data, status) {
			
			console.log(data);
			var obj = $.parseJSON(data);
			// BUGBUG: for some reason this doesn't remove
			//inProcessTable.rows().remove();
			inProcessTable.clear();
			$.each(obj, function() {
				val = 0;
				if (this['workerData']['fibValue'] == -1){
					val = 'in progress';
				
					inProcessTable.row.add([this['workerData']['workerId'],this['workerData']['fibId'],val,this['formattedStartDate'],this['runTime']]).draw();
				}
			});
						
		});
		
		
		$.get('/complete',function(data, status) {
			
			console.log(data);
			var obj = $.parseJSON(data);
			//completeTable.rows().remove();
			completeTable.clear();
			$.each(obj, function() {
				
				completeTable.row.add([this['workerData']['workerId'],this['workerData']['fibId'],this['workerData']['fibValue'],this['workerData']['workerId'],this['formattedStartDate'],this['formattedFinishDate']]).draw();
			});
						
		});
		
		
	}
	
		
	
	loadTable();
	setInterval(function(){ loadTable()},2000);
	
	console.log("ready")
});



