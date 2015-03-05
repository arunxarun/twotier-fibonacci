// main.js


        
$(document).ready(function() {
	
	var t = $('#messages').DataTable( {
        "order": [[ 2, "desc" ]]
    });
	

	function loadTable() {
		$.get('/received',function(data, status) {
			
			console.log(data);
			var obj = $.parseJSON(data);
			t.rows().remove();
			$.each(obj, function() {
				
				val = 0;
				if (this['fibData']['fibValue'] == -1){
					val = 'in progress';
				}
				else {
					
					val = this['fibData']['fibValue'];
				}
				t.row.add([this['fibData']['fibId'],val,this['formattedStartDate'],this['formattedFinishDate']]).draw();
				});
						
		});
	}
	
	$('#submit_button').click(function(e) {
		var url = "/fib";
		
		$.ajax({
		  type: "POST",
		  url: url,
		  data: JSON.stringify({"number": $("#gen_fib").val()}),
		  contentType: "application/json",
          dataType: "json",
		  success: function(data){
		  	console.log(data);
		  	$("#result").html("<span>The number you sent was: "+$("#gen_fib").val()+ ". The returned value was: "+data.fib_value+"</span>");
		  	
		  }
		});

		
	});
	
		
	
	loadTable();
	setInterval(function(){ loadTable()},2000);
	
	console.log("ready")
});



