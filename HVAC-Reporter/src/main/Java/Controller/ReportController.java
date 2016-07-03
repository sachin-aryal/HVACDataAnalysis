package Controller;

import Service.DataService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by iam on 7/2/16.
 */
public class ReportController extends HttpServlet{
    private DataService dataService = null;


    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        if(req.getParameter("buildingId")!=null){
            System.out.println("Getting Drill Request");
            dataService = new DataService();
            int buildingId = Integer.parseInt(req.getParameter("buildingId"));
            req.setAttribute("processedData",dataService.fetchData(buildingId));
            req.setAttribute("classifier","drill");
        }else{
            dataService = new DataService();
            req.setAttribute("processedData",dataService.fetchData());
            req.setAttribute("classifier","noDrill");
        }
        req.getRequestDispatcher("report.jsp").forward(req,resp);

    }

    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
       doGet(req,resp);
    }
}
