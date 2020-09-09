package com.xiaoying.common.mq.xml;

import com.ctrip.framework.apollo.mockserver.EmbeddedApollo;
import org.junit.ClassRule;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * @Author: misskey
 * @Date: 2019-02-28
 * @Version 1.0
 */
public class BaseTest extends AbstractJUnit4SpringContextTests {

    @ClassRule
    public static EmbeddedApollo embeddedApollo = new EmbeddedApollo();


}
