package com.example.kafkapatterns.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binds {@code classpath:rules.yml} ({@code messaging.*}).
 * Topics and consumer groups are no longer hardcoded in {@code KafkaTopics}.
 */
@ConfigurationProperties(prefix = "messaging")
public class MessagingRulesProperties {

    private Topics topics = new Topics();
    private Partitions partitions = new Partitions();
    private Groups groups = new Groups();
    private Map<String, UserRule> users = new LinkedHashMap<>();

    public Topics getTopics() {
        return topics;
    }

    public void setTopics(Topics topics) {
        this.topics = topics;
    }

    public Partitions getPartitions() {
        return partitions;
    }

    public void setPartitions(Partitions partitions) {
        this.partitions = partitions;
    }

    public Groups getGroups() {
        return groups;
    }

    public void setGroups(Groups groups) {
        this.groups = groups;
    }

    public Map<String, UserRule> getUsers() {
        return users;
    }

    public void setUsers(Map<String, UserRule> users) {
        this.users = users;
    }

    public static class Topics {
        private String taskQueue;
        private String broadcast;
        private String orderEvents;
        private String userFootprint;

        public String getTaskQueue() {
            return taskQueue;
        }

        public void setTaskQueue(String taskQueue) {
            this.taskQueue = taskQueue;
        }

        public String getBroadcast() {
            return broadcast;
        }

        public void setBroadcast(String broadcast) {
            this.broadcast = broadcast;
        }

        public String getOrderEvents() {
            return orderEvents;
        }

        public void setOrderEvents(String orderEvents) {
            this.orderEvents = orderEvents;
        }

        public String getUserFootprint() {
            return userFootprint;
        }

        public void setUserFootprint(String userFootprint) {
            this.userFootprint = userFootprint;
        }
    }

    public static class Partitions {
        private int taskQueue = 3;
        private int broadcast = 3;
        private int orderEvents = 3;
        private int userFootprint = 3;

        public int getTaskQueue() {
            return taskQueue;
        }

        public void setTaskQueue(int taskQueue) {
            this.taskQueue = taskQueue;
        }

        public int getBroadcast() {
            return broadcast;
        }

        public void setBroadcast(int broadcast) {
            this.broadcast = broadcast;
        }

        public int getOrderEvents() {
            return orderEvents;
        }

        public void setOrderEvents(int orderEvents) {
            this.orderEvents = orderEvents;
        }

        public int getUserFootprint() {
            return userFootprint;
        }

        public void setUserFootprint(int userFootprint) {
            this.userFootprint = userFootprint;
        }
    }

    public static class Groups {
        private String worker;
        private String analytics;
        private String notifications;
        private String orderLive;
        private Map<String, String> orderPull = new LinkedHashMap<>();
        private String orderReplayPrefix;
        private String userFootprintLive;
        private String userFootprintReplayPrefix;

        public String getWorker() {
            return worker;
        }

        public void setWorker(String worker) {
            this.worker = worker;
        }

        public String getAnalytics() {
            return analytics;
        }

        public void setAnalytics(String analytics) {
            this.analytics = analytics;
        }

        public String getNotifications() {
            return notifications;
        }

        public void setNotifications(String notifications) {
            this.notifications = notifications;
        }

        public String getOrderLive() {
            return orderLive;
        }

        public void setOrderLive(String orderLive) {
            this.orderLive = orderLive;
        }

        public Map<String, String> getOrderPull() {
            return orderPull;
        }

        public void setOrderPull(Map<String, String> orderPull) {
            this.orderPull = orderPull;
        }

        public String getOrderReplayPrefix() {
            return orderReplayPrefix;
        }

        public void setOrderReplayPrefix(String orderReplayPrefix) {
            this.orderReplayPrefix = orderReplayPrefix;
        }

        public String getUserFootprintLive() {
            return userFootprintLive;
        }

        public void setUserFootprintLive(String userFootprintLive) {
            this.userFootprintLive = userFootprintLive;
        }

        public String getUserFootprintReplayPrefix() {
            return userFootprintReplayPrefix;
        }

        public void setUserFootprintReplayPrefix(String userFootprintReplayPrefix) {
            this.userFootprintReplayPrefix = userFootprintReplayPrefix;
        }
    }

    public static class UserRule {
        private String role;
        private String pattern;
        private String delivery;
        private String group;
        private String inbox;
        private String worker;
        private String page;

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }

        public String getPattern() {
            return pattern;
        }

        public void setPattern(String pattern) {
            this.pattern = pattern;
        }

        public String getDelivery() {
            return delivery;
        }

        public void setDelivery(String delivery) {
            this.delivery = delivery;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getInbox() {
            return inbox;
        }

        public void setInbox(String inbox) {
            this.inbox = inbox;
        }

        public String getWorker() {
            return worker;
        }

        public void setWorker(String worker) {
            this.worker = worker;
        }

        public String getPage() {
            return page;
        }

        public void setPage(String page) {
            this.page = page;
        }
    }
}
