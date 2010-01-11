function handler(errorString, exception)
{
  alert(errorString);
}

DWREngine.setErrorHandler(handler);

// doSearch method
function doSearch()
{
  // var searchRequest = new Object();
  var searchRequest = { query:null};
  searchRequest.query = dwr.util.getValue("query");
  SearchService.search(searchRequest, handleDoSearch);
}

function removeChildren(cell)
{		    	
	if ( cell.hasChildNodes() )
	{
	    while ( cell.childNodes.length >= 1 )
	    {
	        cell.removeChild( cell.firstChild );       
	    } 
	}
}


function renderDoc(doc, elem)
{
    var flds = doc.fields;
	var path = "/";  //flds.path[0];
    var user = flds.user;
    var numFollow = flds.num_followers;
    var timestamp = flds.timestamp;
    var tweet = flds.content;
	var frag=flds.fragment[0];
	
	var score=doc.score.toFixed(5);
	
	var nameStr = "<a class=\"hitlink\" href=\"http://www.twitter.com/"+user+"\">" + user + "</a>";
	var fragStr = "<div class=\"frag\">" + frag + "</div>";;
	var numStr  = "(with " + numFollow + " followers) tweeted ";
    var timeStr = " " + timestamp + " ago</div>";
	elem.innerHTML = fragStr + "<div class=\"user\">" + nameStr +  numStr + timeStr;
}
		    
function handleDoSearch(result)
{
   $("#hitcount").show().html("<b>"+result.totalHits+"</b> / <b>"+result.totalDocs+"</b>");
   $("#time").show().html("<b>"+result.time+"</b>");
		//		var elem=document.getElementById("results");
		
   var elem=document.getElementById("results");
				
	var table=document.createElement("table");
	table.width="100%";
	table.id="resTable";
	var docs=result.hits;
	
	removeChildren(elem);
	
	for (var i=0;i<docs.length;++i)
	{
		var row=document.createElement("tr");
		table.appendChild(row);
		var doc=docs[i];
		
		var col=document.createElement("td");
		row.appendChild(col);
		
		renderDoc(doc,col);
	}
	elem.appendChild(table);
}

