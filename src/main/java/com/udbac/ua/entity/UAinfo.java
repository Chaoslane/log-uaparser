package com.udbac.ua.entity;

/**
 * Created by root on 2017/2/20.
 */
public class UAinfo {
    private String catalog = "Other";
    private String browser = "Other";
    private String browserver;
    private String os = "Other";
    private String osver = "";
    private String device = "Other";
    private String brand = "";
    private String model = "";

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public void setBrowserver(String browserver) {
        this.browserver = browserver;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public void setOsver(String osver) {
        this.osver = osver;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getBrowser() {
        return browser;
    }

    public String getBrowserver() {
        return browserver;
    }

    public String getOs() {
        return os;
    }

    public String getOsver() {
        return osver;
    }

    public String getDevice() {
        return device;
    }

    public String getBrand() {
        return brand;
    }

    public String getModel() {
        return model;
    }

    @Override
    public String toString() {
        return catalog + '\t' +
                browser + '\t' +
                browserver + '\t' +
                os + '\t' +
                osver + '\t' +
                device + '\t' +
                brand + '\t' +
                model;
    }
}
