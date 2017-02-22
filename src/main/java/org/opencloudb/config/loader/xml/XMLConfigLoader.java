/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.config.loader.xml;

import java.util.Map;

import org.opencloudb.config.loader.ConfigLoader;
import org.opencloudb.config.loader.SchemaLoader;
import org.opencloudb.config.model.ClusterConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.config.model.DataNodeConfig;
import org.opencloudb.config.model.FirewallConfig;
import org.opencloudb.config.model.QuarantineConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

/**
 * @author mycat
 */
public class XMLConfigLoader implements ConfigLoader {

    /** unmodifiable */
    private final Map<String, DataHostConfig> dataHosts;
    /** unmodifiable */
    private final Map<String, DataNodeConfig> dataNodes;
    /** unmodifiable */
    private final Map<String, SchemaConfig> schemas;
    private final SystemConfig system;
    private final FirewallConfig firewall;
    /** unmodifiable */
    private final Map<String, UserConfig> users;
    private final QuarantineConfig quarantine;
    private final ClusterConfig cluster;
    
    private final Map<String, TableRuleConfig> tableRules;
    private final Map<String, AbstractPartitionAlgorithm> functions;

    public XMLConfigLoader(SchemaLoader schemaLoader) {
        XMLServerLoader serverLoader = new XMLServerLoader();
        this.system = serverLoader.getSystem();
        this.firewall = serverLoader.getFirewall();
        this.users = serverLoader.getUsers();
        this.quarantine = serverLoader.getQuarantine();
        this.cluster = serverLoader.getCluster();
        this.dataHosts = schemaLoader.getDataHosts();
        this.dataNodes = schemaLoader.getDataNodes();
        this.schemas = schemaLoader.getSchemas();
        this.tableRules = schemaLoader.getTableRules();
        this.functions = schemaLoader.getFunctions();
        schemaLoader = null;
    }

    @Override
    public ClusterConfig getClusterConfig() {
        return cluster;
    }

    @Override
    public QuarantineConfig getQuarantineConfig() {
        return quarantine;
    }

    @Override
    public UserConfig getUserConfig(String user) {
        return users.get(user);
    }

    @Override
    public Map<String, UserConfig> getUserConfigs() {
        return users;
    }

    @Override
    public SystemConfig getSystemConfig() {
        return system;
    }
    
    @Override
    public FirewallConfig getFirewallConfig() {
    	return firewall;
    }
    
    @Override
    public Map<String, SchemaConfig> getSchemaConfigs() {
        return schemas;
    }

    @Override
    public Map<String, DataNodeConfig> getDataNodes() {
        return dataNodes;
    }

    @Override
    public Map<String, DataHostConfig> getDataHosts() {
        return dataHosts;
    }

    @Override
    public SchemaConfig getSchemaConfig(String schema) {
        return schemas.get(schema);
    }

	@Override
	public Map<String, TableRuleConfig> getTableRules() {
		return tableRules;
	}

	@Override
	public Map<String, AbstractPartitionAlgorithm> getFunctions() {
		return functions;
	}

}