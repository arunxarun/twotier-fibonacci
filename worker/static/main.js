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
			inProcessTable.rows().remove();
			$.each(obj, function() {
				val = 0;
				if (this['fibData']['fibValue'] == -1){
					val = 'in progress';
				}
				else {
					
					val = this['fibData']['fibValue'];
				}
				inProcessTable.row.add([this['fibData']['fibId'],val,this['formattedStartDate'],this['runTime']]).draw();
			});
						
		});
		
		
		$.get('/complete',function(data, status) {
			
			console.log(data);
			var obj = $.parseJSON(data);
			completeTable.rows().remove();
			$.each(obj, function() {
				
				completeTable.row.add([this['fibData']['fibId'],this['fibData']['fibValue'],this['fibData']['workerId'],this['formattedStartDate'],this['formattedFinishDate']]).draw();
			});
						
		});
		
		
	}
	
		
	
	loadTable();
	setInterval(function(){ loadTable()},2000);
	
	console.log("ready")
});



