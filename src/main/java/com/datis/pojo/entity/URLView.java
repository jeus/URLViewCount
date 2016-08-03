/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.pojo.entity;

/**
 *
 * @author jeus
 */
public class URLView {

    private String url = "";
    private String user = "";
    private String region = "";

    public URLView() {

        url = "yahoo";
        user = "Ali";
        region = "iran";
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "URLView{" + "url=" + url + ", user=" + user + ", region=" + region + '}';
    }

}
