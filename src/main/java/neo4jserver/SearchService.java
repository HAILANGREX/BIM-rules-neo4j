package neo4jserver;

import org.neo4j.cypher.internal.expressions.In;
import org.neo4j.driver.*;
import org.neo4j.fabric.stream.StatementResult;
import javax.annotation.Resource;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.neo4j.driver.Record;
import scala.Int;

import static org.neo4j.driver.Values.parameters;

public class SearchService {

    private Driver driver;
    private Session session;
    private Map<String,String> factor = new HashMap<>();

    public SearchService(String url,String username,String password){
        driver = GraphDatabase.driver( url, AuthTokens.basic( username, password ) );
        session = driver.session();
    }
    /**
    * @Description: 删除所有数据
    * @Param: []
    * @return: void
    * @Author: Wang
    * @Date: 2021/8/19
    */
    public void delete_all(){
        session.run( "match (n) detach delete n");
    }

    public void add_point(){
        session.run( "CREATE (a:Person {name: $name, title: $title})",parameters( "name", "San Zhang", "title", "person" ) );
    }


    public void add_edge(){
        session.run( "MATCH(n1),(n2) WHERE n1.name='San Zhang' AND n2.name='San Zhang' CREATE (n1)-[:test] -> (n2)" );
//                "CREATE (a:Person {name: $name, title: $title})",parameters( "name", "San Zhang", "title", "person" ) );
    }
    public void add_edge_with_id(){
        session.run( "MATCH(n1),(n2) WHERE id(n1)= 10 AND id(n2)=11  CREATE (n1)-[:test{name:'relate'}] -> (n2)" );
//                "CREATE (a:Person {name: $name, title: $title})",parameters( "name", "San Zhang", "title", "person" ) );
    }

    /**
    * @Description: 创建节点实体
    * @Param: [components]
    * @return: void
    * @Author: Wang
    * @Date: 2021/8/19
    */
    public void create_entity(List<List<String>> components){
        Iterator<List<String>> it = components.iterator();
        while (it.hasNext()) {
            List<String> str = (List<String>) it.next();
            session.run( "CREATE (a:Component_Entity {name: $name, entity_type: $entity_type, omniclass: $omniclass, national_standard: $national_standard, ifc_type: $ifc_type, describe: $describe, note: $note})",parameters( "name", str.get(0), "entity_type", str.get(1), "omniclass", str.get(2), "national_standard", str.get(3), "ifc_type", str.get(4), "describe", str.get(5), "note", str.get(6) ) );
        }

    }

    public void read_factor(List<List<String>> factors){
        Iterator<List<String>> it = factors.iterator();
        while (it.hasNext()) {
            List<String> str = (List<String>) it.next();
            factor.put(str.get(3),str.get(2));
        }

    }
    /**
    * @Description: 查询语句
    * @Param: []
    * @return: void
    * @Author: Wang
    * @Date: 2021/8/19
    */
    public void return_result(){
        Result result = session.run( "MATCH (a:Component_Entity) WHERE a.name = '梁' " + "RETURN a.name AS name, id(a) AS id",
                parameters( "name", "梁" ) );

        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "id" ).asInt() + " " + record.get( "name" ).asString() );
        }
    }




    /**
    * @Description: 创建规则关系
    * @Param: [components]
    * @return: void
    * @Author: Wang
    * @Date: 2021/8/19
    */
    public void create_rules(List<List<String>> rules){

        List<List<String>> rule = new ArrayList<>();
        Iterator<List<String>> it = rules.iterator();

//        flag:
        while (it.hasNext()) {
            List<String> str = (List<String>) it.next();
            while (str.get(2).equals("IF")||str.get(2).equals("")||str.get(2).equals("IF*"))
            {
                if (rule.size()!=0){
                    create_rule(rule);
                    rule.clear();
//                    continue flag;
                }
                break;
            }
            rule.add(str);

//            session.run( "CREATE (a:Component_Entity {name: $name, entity_type: $entity_type, omniclass: $omniclass, national_standard: $national_standard, ifc_type: $ifc_type, describe: $describe, note: $note})",parameters( "name", str.get(0), "entity_type", str.get(1), "omniclass", str.get(2), "national_standard", str.get(3), "ifc_type", str.get(4), "describe", str.get(5), "note", str.get(6) ) );
        }

    }


    public void create_rule(List<List<String>> rule){

        if(rule.size()==1)
        {
            create_one_rule(rule.get(0));
        }
        else
            create_many_rule(rule);
//          System.out.println(rule);
    }

    public void create_one_rule(List<String> rule){
        session.run( "MATCH (a:Component_Entity) WHERE a.entity_type = $entity_type " +
                        "CREATE (a)-[:Final_Rule{rule_num: $rule_num,belong:$belong}]->(b:Rule_Start{name:'Start',rule_num:'1',rule_mode:'and'})"+
                "WITH b CREATE (b)-[:Require_Rule{rule_num: $rule_num,belong:$belong, name:$require_name ,require_type: $require_type}]->(c:Require_Entity{compare_type: $compare_type,name: $require_value})"+
                "WITH c CREATE (c)-[:Stop_Rule{rule_num: $rule_num,belong:$belong}]->(d:Rule_End{name:'End',rule_num: '1'})",
                parameters( "entity_type", rule.get(3),"belong",rule.get(3),"rule_num",rule.get(0),"require_name",factor.get(rule.get(4)),"require_type",rule.get(4),"compare_type",rule.get(5),"require_value",rule.get(6) ) );
   }

    public void create_many_rule(List<List<String>> rule){

        Iterator<List<String>> it = rule.iterator();
        Map<String,List<List<String>>> prerules = new HashMap<>();
        String rule_num = rule.get(0).get(0);
        String finalEntity = null;
        List<String> finalRule = new ArrayList<>();

        out: while (it.hasNext()) {
            List<String> str = (List<String>) it.next();
            String entityName = str.get(3);
            while (str.get(2).equals("THEN")||str.get(2).equals("THEN*"))
            {
                finalEntity = str.get(3);
                finalRule = str;

                break out;
            }
            if(prerules.containsKey(entityName))
            {
                prerules.get(entityName).add(str);
            }else {
                List<List<String>> lists = new ArrayList<>();
                lists.add(str);
                prerules.put(entityName,lists);
            }

//            session.run( "CREATE (a:Component_Entity {name: $name, entity_type: $entity_type, omniclass: $omniclass, national_standard: $national_standard, ifc_type: $ifc_type, describe: $describe, note: $note})",parameters( "name", str.get(0), "entity_type", str.get(1), "omniclass", str.get(2), "national_standard", str.get(3), "ifc_type", str.get(4), "describe", str.get(5), "note", str.get(6) ) );
        }
        System.out.println(prerules);
        System.out.println(finalEntity);

        Integer relay = null;

        if(prerules.containsKey(finalEntity))
        {
            Set<String> ruleSet = prerules.keySet();
            if(prerules.keySet().size()==1)
            {
                Integer startId = set_rule_start_node();
                Integer endId = set_rule_end_node();
                relay = endId;
                Integer finalId = find_node_by_keyvalue("Component_Entity","entity_type",finalEntity);
                set_relation_by_twoNodes(finalId,startId,"Private_Rule",rule_num,finalEntity);
                for (List<String> requireRule : prerules.get(finalEntity))
                {
                    set_require_relation(startId,endId,rule_num,requireRule,finalEntity);
                }

                Integer startId2 = set_rule_start_node();
                Integer endId2 = set_rule_end_node();

                set_relation_by_twoNodes(endId,startId2,"Final_Rule",rule_num,finalEntity);
                set_require_relation(startId2,endId2,rule_num,finalRule,finalEntity);
            }
            else {
                Integer startId = set_rule_start_node();
                Integer endId = set_rule_end_node();
                relay = endId;
                Integer finalId = find_node_by_keyvalue("Component_Entity","entity_type",finalEntity);
                set_relation_by_twoNodes(finalId,startId,"Private_Rule",rule_num,finalEntity);
                for (List<String> requireRule : prerules.get(finalEntity))
                {
                    set_require_relation(startId,endId,rule_num,requireRule,finalEntity);
                }
                ruleSet.remove(finalEntity);
                for (String entity:ruleSet)
                {
                    Integer startId2 = set_rule_start_node();
                    Integer endId2 = set_rule_end_node();
                    Integer preEntity = find_node_by_keyvalue("Component_Entity","entity_type",entity);
                    set_relation_by_twoNodes(relay,preEntity,"Pre_Rule",rule_num,finalEntity);
                    set_relation_by_twoNodes(preEntity,startId2,"Private_Rule",rule_num,finalEntity);
                    relay = endId2;
                    for (List<String> requireRule : prerules.get(entity))
                    {
                        set_require_relation(startId2,endId2,rule_num,requireRule,finalEntity);
                    }
                }
                Integer startId2 = set_rule_start_node();
                Integer endId2 = set_rule_end_node();
                set_relation_by_twoNodes(relay,startId2,"Final_Rule",rule_num,finalEntity);
                set_require_relation(startId2,endId2,rule_num,finalRule,finalEntity);
            }
        }
        else {
            relay = find_node_by_keyvalue("Component_Entity","entity_type",finalEntity);
            for (String entity:prerules.keySet())
            {
                Integer startId2 = set_rule_start_node();
                Integer endId2 = set_rule_end_node();
                Integer preEntity = find_node_by_keyvalue("Component_Entity","entity_type",entity);
                set_relation_by_twoNodes(relay,preEntity,"Pre_Rule",rule_num,finalEntity);
                set_relation_by_twoNodes(preEntity,startId2,"Private_Rule",rule_num,finalEntity);
                relay = endId2;
                for (List<String> requireRule : prerules.get(entity))
                {
                    set_require_relation(startId2,endId2,rule_num,requireRule,finalEntity);
                }
            }
            Integer startId2 = set_rule_start_node();
            Integer endId2 = set_rule_end_node();
            set_relation_by_twoNodes(relay,startId2,"Final_Rule",rule_num,finalEntity);
            set_require_relation(startId2,endId2,rule_num,finalRule,finalEntity);
        }

   }


    /**
    * @Description: 根据key、value查询节点id
    * @Param: [type, key, value]
    * @return: java.lang.Integer
    * @Author: Wang
    * @Date: 2021/8/20
    */
    public Integer find_node_by_keyvalue(String type,String key,String value){
        Result result = session.run( "MATCH (a:"+type+") WHERE a."+key+" = $value RETURN id(a) AS id " ,
                parameters("value",value));

        while ( result.hasNext() )
        {
            Record record = result.next();
//            System.out.println( record.get( "id" ).asInt() + " " + record.get( "name" ).asString() );
            return record.get("id").asInt();
        }
        return null;
    }


    public void set_relation_by_twoNodes(Integer startNode,Integer endNode,String relationType,String rule_num,String belong){
        session.run( "MATCH (a),(b) WHERE id(a) = "+startNode+" AND id(b) = "+endNode+
                        " CREATE (a)-[:"+relationType+"{rule_num:$rule_num,belong:$belong}]->(b) " ,
                parameters("rule_num",rule_num,"belong",belong));
    }

    public Integer set_rule_start_node()
    {
        Result result = session.run( "CREATE (n:Rule_Start{name: 'Start',rule_mode: 'and'}) RETURN id(n) AS id" );

        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "id" ).asInt() + " " + record.get( "name" ).asString() );
            return record.get("id").asInt();
        }
        return null;
    }

    public Integer set_rule_end_node()
    {
        Result result = session.run( "CREATE (n:Rule_End{name: 'End'}) RETURN id(n) AS id" );

        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "id" ).asInt() + " " + record.get( "name" ).asString() );
            return record.get("id").asInt();
        }
        return null;
    }

    
    /** 
    * @Description: 设置require关系
    * @Param: [startId, endId, requireName, requireType, rule_num, require_value, compareType]
    * @return: void
    * @Author: Wang
    * @Date: 2021/8/19
    */
    public void set_require_relation(Integer startId,Integer endId,String rule_num,List<String> rules,String belong){
        Result result = session.run( "MATCH (n1),(n2) WHERE id(n1)= $start AND id(n2)= $end" +
                " CREATE (n1)-[:Require_Rule{rule_num: $rule_num, belong:$belong,name:$require_name ,require_type: $require_type}]->(c:Require_Entity{compare_type: $compare_type,name: $require_value})" +
                        "WITH c,n2 CREATE (c)-[:Stop_Rule{rule_num: $rule_num,belong:$belong}]->(n2)",
                parameters( "start", startId, "end",endId,"rule_num",rule_num,"require_name",factor.get(rules.get(4)),
                        "require_type",rules.get(4),"require_value",rules.get(6),"compare_type",rules.get(5),"belong",belong ));

        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "id" ).asInt() + " " + record.get( "name" ).asString() );
        }
    }
    /**
    * @Description: 关闭服务
    * @Param: []
    * @return: void
    * @Author: Wang
    * @Date: 2021/8/19
    */
    public void service_close(){
        session.close();
        driver.close();
    }

}
