document.getElementById("routeForm").addEventListener("submit", function(event){
    event.preventDefault();
    const submitButton = document.getElementById("submit");
    const spinner = document.getElementById("loadingIndicator");
    submitButton.disabled = true;
    spinner.style='';
    const origin = document.getElementById("origin").value;
    const destination = document.getElementById("destination").value;
    const url = `/route?origin=${encodeURIComponent(origin)}&dest=${encodeURIComponent(destination)}`;

    fetch(url)
        .then(response => response.json())
        .then(data => {
            populateTable('leastDelayedDirect', data.leastDelayedDirect);
            populateTable('leastCanceledDirect', data.leastCanceledDirect);
            populateTable('leastDelayedOneHop', data.leastDelayedOneHop);
            renderTimingData(data)
        })
        .catch(error => {
            console.error('Error:', error)
            document.getElementById('error').textContent ='An error occurred with this search. Please try a different origin or destination'+error
        }).finally(()=>{
            submitButton.disabled = false;
            spinner.style.display = 'none';
            document.getElementById("timingList").style=''
            document.getElementById("timingCover").style='display:none'
        });
});


function renderTimingData(data){

    document.getElementById("timeToCalculateDirectRoutes").textContent=data.timeToCalculateDirectRoutes/1000;
    document.getElementById("timeToCalculateOneStopRoutes").textContent=data.timeToCalculateOneStopRoutes/1000;
    document.getElementById("totalTime").textContent=data.totalTime/1000;

}

function populateTable(tableId, routes) {
    const table = document.getElementById(tableId);
    // clear the table
    table.innerHTML = '';
    if(routes ==null || routes.length === 0) {
        table.innerHTML = '<tr><td>No data available</td></tr>';
        return;
    }
    if(tableId == "leastDelayedOneHop"){
        let headerRow = '<tr><th>Origin</th><th>Layover</th><th>Final Destination</th>'+
        '<th>Carrier</th><th>Avg Delay percentage to layover</th> <th>Avg delay longer than 15 in minutes to layover</th>'
        + '<th>Avg delay percentage to final</th> <th>Avg delay longer than 15 in minutes to final</th>'
        '</tr>';
        table.innerHTML = headerRow;
        routes.forEach(route => {
            let row = `<tr>
                    <td>${route[0].origin}</td>
                    <td>${route[0].dest}</td>
                    <td>${route[1].dest}</td>
                    <td>${route[0].carrierName}</td>
                    <td>${route[0].percentageDelayedLongerThan15}</td>
                    <td>${route[0].avgDelayLongerThan15}</td>
                    <td>${route[1].percentageDelayedLongerThan15}</td>
                    <td>${route[1].avgDelayLongerThan15}</td>
                </tr>`;
                table.innerHTML += row;
        });
    }else{
    //it's a direct route, no need for fancy parsing
        const headers = Object.keys(routes[0]);
        let headerRow = '<tr>';
        headers.forEach(header => headerRow += `<th>${header}</th>`);
        headerRow += '</tr>';
        table.innerHTML = headerRow;
        routes.forEach(flight => {
            let row = '<tr>';
            headers.forEach(header => row += `<td>${JSON.stringify(flight[header])}</td>`);
            row += '</tr>';
            table.innerHTML += row;
        });
    }
}
