package org.opencloudb.config.loader.xml.jaxb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

import org.opencloudb.config.model.rule.RuleConfig;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.manager.response.ListFunctions;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;


@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://org.opencloudb/", name = "rule")
public class RuleJAXB {

    @XmlElement(name = "tableRule")
	private List<TableRule> tableRules = new ArrayList<TableRule>();
	
	@XmlElement(name = "function")
	private List<Function> functions = new ArrayList<Function>();
	
	public RuleJAXB(){
	}
	
	public RuleJAXB(Map<String, TableRuleConfig> currentRules, Map<String, AbstractPartitionAlgorithm> currentFunctions) {
		
		for (String name : currentRules.keySet()) {
			tableRules.add(new TableRule(name, currentRules.get(name)));
		}
		
		for (String name : currentFunctions.keySet()) {
			functions.add(new Function(name, currentFunctions.get(name)));
		}
	}
	
	@XmlAccessorType(XmlAccessType.FIELD)
	@XmlType(name = "tableRule")
	public static class TableRule {
		@XmlAttribute(required = true)
		private String name;
		
		@XmlElement(name = "rule")
		private Rule rule;
		
		public TableRule(String name, TableRuleConfig tableRule) {
			this.name = name;
			
			this.rule = new Rule(tableRule.getRule());
		}
		
		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Rule getRule() {
			return rule;
		}

		public void setRule(Rule rule) {
			this.rule = rule;
		}

		@XmlAccessorType(XmlAccessType.FIELD) 
		@XmlType(name = "rule") 
		public static class Rule {
			@XmlElement(name = "columns")
			private Columns columns;
			
			@XmlElement(name = "algorithm")
			private Algorithm algorithm;
			
			public Rule(RuleConfig rule) {
				columns = new Columns(rule.getColumn());
				algorithm = new Algorithm(rule.getFunctionName());
			}
			
			public Columns getColumns() {
				return columns;
			}

			public void setColumns(Columns columns) {
				this.columns = columns;
			}

			public Algorithm getAlgorithm() {
				return algorithm;
			}

			public void setAlgorithm(Algorithm algorithm) {
				this.algorithm = algorithm;
			}

			@XmlAccessorType(XmlAccessType.FIELD) 
			@XmlType(name = "columns")
			public static class Columns {
				@XmlValue 
			    protected String value;
				
				public Columns(String value) {
					this.value = value;
				}

				public String getValue() {
					return value;
				}

				public void setValue(String value) {
					this.value = value;
				}
			}
			
			@XmlAccessorType(XmlAccessType.FIELD) 
			@XmlType(name = "algorithm")
			public static class Algorithm {
				@XmlValue 
			    protected String value;
				
				public Algorithm(String value) {
					this.value = value;
				}

				public String getValue() {
					return value;
				}

				public void setValue(String value) {
					this.value = value;
				}
			}
		}
		
	}
	
	@XmlAccessorType(XmlAccessType.FIELD)
	@XmlType(name = "function")
	public static class Function {
		@XmlAttribute(required = true)
		private String name;
		
		@XmlAttribute(name="class", required = true)
		private String className;
		
		@XmlElement(name = "property")
		private List<Property> properties = new ArrayList<Property>();
		
		public Function(String name, AbstractPartitionAlgorithm function) {
			this.name = name;
			
			this.className = function.getClass().getName();
			
			Map<String, Object> funcProperities = ListFunctions.acquireProperties(function);
			for (String propName : funcProperities.keySet()) {
				this.properties.add(new Property(propName, funcProperities.get(propName).toString()));
			}
		}
		

		public List<Property> getProperties() {
			return properties;
		}

		public void setProperties(List<Property> properties) {
			this.properties = properties;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getClassName() {
			return className;
		}

		public void setClassName(String className) {
			this.className = className;
		}
		
	}

	public List<TableRule> getTableRules() {
		return tableRules;
	}

	public void setTableRules(List<TableRule> tableRules) {
		this.tableRules = tableRules;
	}

	public List<Function> getFunctions() {
		return functions;
	}

	public void setFunctions(List<Function> functions) {
		this.functions = functions;
	}
}
