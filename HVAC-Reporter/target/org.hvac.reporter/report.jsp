<%@ page import="Model.HVAC" %>
<%@ page import="java.util.List" %><%--
  Created by IntelliJ IDEA.
  User: iam
  Date: 7/2/16
  Time: 12:11 PM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>HVAC System</title>
    <script src="js/jquery-1.10.2.js"></script>
    <script src="https://cdn.datatables.net/1.10.12/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/scroller/1.4.2/js/dataTables.scroller.min.js"></script>
    <script src="js/bootstrap.min.js"></script>
    <link href="https://cdn.datatables.net/1.10.12/css/jquery.dataTables.min.css" rel="stylesheet"/>
    <link href="css/bootstrap.min.css" rel="stylesheet"/>
    <link href="https://cdn.datatables.net/scroller/1.4.2/css/scroller.dataTables.min.css" rel="stylesheet"/>


    <script type="text/javascript">
        $(function () {
            $("#initialResult").DataTable();
            $("#drillResult").DataTable();
        })
    </script>

    <style>
        input[type="search"] {
            color: black;
        }
        select {
            color: black;
        }
    </style>

</head>
<body>
<a href="/report" class="btn btn-primary" style="margin-left: 65px;margin-top: 20px">Home</a>
<%
    String classifier = (String) request.getAttribute("classifier");
    List<HVAC> processedData = (List<HVAC>) request.getAttribute("processedData");

    if(classifier.equalsIgnoreCase("drill")){


%>
<div id="drillTable" style="margin:0 auto;width:90%;margin-top: 20px">
    <table id="drillResult" class="display table table-bordered" width="100%">
        <thead>
        <tr>
            <th>Building Id</th>
            <th>Manager</th>
            <th>Modal No</th>
            <th>Recorded Date</th>
            <th>Recorded Time</th>
            <th>Expected Data</th>
            <th>Result Data</th>
        </tr>
        </thead>
        <tbody>
        <%
            if(processedData!=null){
                for(HVAC hvac:processedData){
                    out.println("<tr>");
                    out.println("<td>"+hvac.getBuildingId()+"</td>");
                    out.println("<td>"+hvac.getManager()+"</td>");
                    out.println("<td>"+hvac.getModalNo()+"</td>");
                    out.println("<td>"+hvac.getRecordDate()+"</td>");
                    out.println("<td>"+hvac.getRecordTime()+"</td>");
                    out.println("<td>"+hvac.getExpectedData()+"</td>");
                    out.println("<td>"+hvac.getResultedData()+"</td>");
                    out.println("</tr>");
                }
            }else{
                out.print("<h1 style='color:red'>No Data Received.</h1>");
            }
        %>
        </tbody>
    </table>
</div>
<%
}else{
%>
<div id="initialTable" style="margin:0 auto;width:90%;margin-top: 20px">
    <table id="initialResult" class="display table table-bordered" width="100%">
        <thead>
        <tr>
            <th>Building Id</th>
            <th>Modal Number</th>
            <th>Expected Data</th>
            <th>Result Data</th>
        </tr>
        </thead>
        <tbody>
        <%
            if(processedData!=null){
                for(HVAC hvac:processedData){
                    out.println("<tr>");
                    out.println(
                            "<td>"+
                                    "<a href='/report?buildingId="+hvac.getBuildingId()+"'/>" +hvac.getBuildingId()+
                                    "</a></td>"
                    );
                    out.println("<td>"+hvac.getModalNo()+"</td>");
                    out.println("<td>"+hvac.getExpectedData()+"</td>");
                    out.println("<td>"+hvac.getResultedData()+"</td>");
                    out.println("</tr>");
                }
            }else{
                out.print("<h1 style='color:red'>No Data Received.</h1>");
            }
        %>
        </tbody>
    </table>
</div>
<%
    }
%>
<br>
<br>
<br>
<br>
<br>
</body>
</html>
