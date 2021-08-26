package neo4jserver;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.fabric.stream.StatementResult;
import neo4jserver.SearchService;

import java.util.List;

import static org.neo4j.driver.Values.parameters;



public class App
{
    public static void main( String[] args )
    {

//        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "wjlwch" ) );
//        Session session = driver.session();
////        session.run( "CREATE (a:Person {name: $name, title: $title})",parameters( "name", "San Zhang", "title", "person" ) );
//        session.run( "MATCH (a:Person {name: $name1}),(b:Person {name: $name2}) create (a)-[r: hate]->(b) return r",parameters( "name1", "Jialin Wang", "name2", "San Zhang","relation", "hate" ) );
//
//        Result result = session.run( "MATCH (a:Person) WHERE a.name = 'Jialin Wang' " + "RETURN a.name AS name, a.title AS title",
//                parameters( "name", "Jialin Wang" ) );
//
//        while ( result.hasNext() )
//        {
//            Record record = result.next();
//            System.out.println( record.get( "title" ).asString() + " " + record.get( "name" ).asString() );
//        }
//        session.close();
//        driver.close();

        ReadExcel readExcel = new ReadExcel();
        List<List<List<String>>> list = readExcel.readExcelall("E:\\报告\\数据资料整理_4.0.xlsx");
        List<List<String>> components = list.get(0).subList(1,list.get(0).size());
        List<List<String>> factor = list.get(1).subList(1,list.get(1).size());
        List<List<String>> rules = list.get(2).subList(1,list.get(2).size());



        SearchService searchService = new SearchService("neo4j://localhost:7687","neo4j","wjlwch");
        searchService.read_factor(factor);

//        int a = searchService.find_node_by_keyvalue("Component_Entity","entity_type","door");
        searchService.delete_all();
        searchService.create_entity(components);
        searchService.create_rules(rules);

//        searchService.add_point();
//        searchService.add_edge_with_id();
//        searchService.return_result();
        searchService.service_close();
    }
}