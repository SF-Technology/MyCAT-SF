package org.opencloudb.config.model;

import java.util.Arrays;
import java.util.List;

import org.opencloudb.util.SplitUtil;

/**
 * 封装schema.xml procedure配置
 * @author CrazyPig
 * @since 1.5.3
 *
 */
public class ProcedureConfig {

    private String name;
    private String dataNode;
    private List<String> dataNodes;
    private boolean hasMultiNode;

    public ProcedureConfig(String name, String dataNode) {
        this.name = name;
        this.dataNode = dataNode;
        String theDataNodes[] = SplitUtil.split(dataNode, ',', '$', '-');
        if (theDataNodes == null || theDataNodes.length <= 0) {
            throw new IllegalArgumentException("invalid procedure dataNodes: " + dataNode);
        }
        this.dataNodes = Arrays.asList(theDataNodes);
        this.hasMultiNode = theDataNodes.length > 1;
    }

    public String getName() {
        return name;
    }

    public String getDataNode() {
        return dataNode;
    }

    public List<String> getDataNodes() {
        return dataNodes;
    }

    public boolean isHasMultiNode() {
        return hasMultiNode;
    }

}
