package org.janstenpickle.flume.channel;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.AbstractChannelSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 06/09/2013
 * Time: 14:51
 * To change this template use File | Settings | File Templates.
 */
public class RegexMultiplexingChannelSelector extends AbstractChannelSelector {

    private static final Logger LOG = LoggerFactory
            .getLogger(RegexMultiplexingChannelSelector.class);

    public static final String CONFIG_MULTIPLEX_HEADER_NAME = "header";
    public static final String DEFAULT_MULTIPLEX_HEADER =
            "flume.selector.header";
    public static final String CONFIG_DEFAULT_CHANNELS = "default";
    public static final String CONFIG_PREFIX_MAPPING = "mapping.";
    public static final String CONFIG_PREFIX_REGEX = "regex";


    private List<Channel> defaultChannels;
    private Map<String, List<Channel>> headerChannelMapping;
    private Map<Pattern, List<Channel>> regexChannelMapping;
    private String headerName;


    @Override
    public void configure(Context context) {
        this.headerName = context.getString(CONFIG_MULTIPLEX_HEADER_NAME,
                DEFAULT_MULTIPLEX_HEADER);

        Map<String, Channel> channelNameMap = getChannelNameMap();

        defaultChannels = getChannelListFromNames(
                context.getString(CONFIG_DEFAULT_CHANNELS), channelNameMap);

        Map<String, String> headerMapConfig =
                context.getSubProperties(CONFIG_PREFIX_MAPPING);

        headerChannelMapping = new HashMap<String, List<Channel>>();

        for (String headerValue : headerMapConfig.keySet()) {
            List<Channel> configuredChannels = getChannelListFromNames(
                    headerMapConfig.get(headerValue),
                    channelNameMap);

            //This should not go to default channel(s)
            //because this seems to be a bad way to configure.
            if (configuredChannels.size() == 0) {
                throw new FlumeException("No channel configured for when "
                        + "header value is: " + headerValue);
            }

            if (headerChannelMapping.put(headerValue, configuredChannels) != null) {
                throw new FlumeException("Selector channel configured twice");
            }
        }

        regexChannelMapping = new HashMap<Pattern, List<Channel>>();

        if (context.getString(CONFIG_PREFIX_REGEX) != null) {
            List<String> regexes = Arrays.asList(context.getString(CONFIG_PREFIX_REGEX).split(" "));


            for (Iterator<String> regexIterator = regexes.iterator(); regexIterator.hasNext(); ) {
                String regexName = regexIterator.next();
                Map<String, String> regexMapConfig =
                        context.getSubProperties(CONFIG_PREFIX_REGEX + "." + regexName + ".");
                String regexValue = regexMapConfig.get("pattern");
                Pattern regex = Pattern.compile(regexValue);
                List<Channel> configuredChannels = getChannelListFromNames(regexMapConfig.get("channels"), channelNameMap);
                if (configuredChannels.size() == 0) {
                    throw new FlumeException("No channel configured for when "
                            + "regex value is: " + regexValue);
                }
                if (regexChannelMapping.put(regex, configuredChannels) != null) {
                    throw new FlumeException("Selector channel configured twice");
                }
            }
        }

    }

    @Override
    public List<Channel> getRequiredChannels(Event event) {

        String headerValue = event.getHeaders().get(headerName);
        List<Channel> channels = new ArrayList<Channel>();

        if ((headerValue != null || headerValue.trim().length() != 0) && headerChannelMapping.containsKey(headerValue)) {

            List<Channel> regexChannels = new ArrayList<Channel>();
            List<Channel> headerChannels = headerChannelMapping.get(headerValue);


            //forwards all events to the default channels always
            channels.addAll(defaultChannels);

            if (regexChannelMapping.size() > 0) {
                for (Iterator<Pattern> regexChannelMappingIterator = regexChannelMapping.keySet().iterator();
                     regexChannelMappingIterator.hasNext(); ) {
                    Pattern regex = regexChannelMappingIterator.next();
                    List<Channel> channelsList = regexChannelMapping.get(regex);
                    for (Iterator<Channel> channelIterator = channelsList.iterator(); channelIterator.hasNext(); ) {
                        Channel channel = channelIterator.next();
                        if (headerChannels.contains(channel)) {
                            if (regex.matcher(new String(event.getBody())).find()) {
                                regexChannels.add(channel);
                            }
                        }
                    }
                }
            } else {
                channels.addAll(headerChannels);
            }
            channels.addAll(regexChannels);
        }


        return channels;
    }

    @Override
    public List<Channel> getOptionalChannels(Event event) {
        return new ArrayList<Channel>();  //To change body of implemented methods use File | Settings | File Templates.
    }
}
