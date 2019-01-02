#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# http://dhime.ideam.gov.co/atencionciudadano/

import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Firefox()
driver.get("http://dhime.ideam.gov.co/atencionciudadano/")
login_checkbox = "/html/body/div[2]/div/div[3]/div[2]/div[2]/div[2]/div[1]/div/div[1]"
login_button = "/html/body/div[2]/div/div[3]/div[2]/div[2]/div[2]/div[3]"
driver.find_element_by_xpath(login_checkbox).click()
driver.find_element_by_xpath(login_button).click()
main_page = driver.find_element_by_xpath('//*[@id="main-page"]')
layout = main_page.find_element_by_xpath('//*[@id="jimu-layout-manager"]')
search_panel = layout.find_element_by_xpath('//*[@id="_20_panel"]')
search_container = search_panel.find_element_by_xpath('/html/body/div[2]/div/div[5]/div[1]')
widget = search_container.find_element_by_xpath('//*[@id="dijit__WidgetBase_0"]')
print(widget.get_attribute('id'))
time.sleep(2)
widget_ciudadano = widget.find_element_by_xpath('//*[@id="widgets_Ciudadano_Widget_38"]')
# contenedor_ciudadano = widget_ciudadano.find_element_by_id('ContenedorCiudadano')
# contenedor_ciudadano_panewrapper = contenedor_ciudadano.find_element_by_xpath(
#     '/html/body/div[2]/div/div[2]/div[1]/div/div/div[1]/div[3]')
# contenedor_ciudadano_tab_panel = contenedor_ciudadano_panewrapper.find_element_by_xpath(
#     '/html/body/div[2]/div/div[2]/div[1]/div/div/div[1]/div[3]/div[1]')
# contenedor_ciudadano_tab_pane = contenedor_ciudadano_tab_panel.find_element_by_xpath('//*[@id="first"]')
#
# contenedor_ciudadano_tablist = contenedor_ciudadano.find_element_by_xpath('//*[@id="ContenedorCiudadano_tablist"]')


# period panel
# print(driver.find_element_by_id("first"))

# searcher = layout.find_element_by_xpath('//*[@id="dijit__WidgetBase_0"]')
# x = driver.find_element_by_css_selector("#datepicker")
start_date = driver.find_element_by_xpath('//*[@id="datepicker"]')
start_date.clear()
start_date.send_keys("01/01/1950")
end_date = driver.find_element_by_xpath('//*[@id="datepicker1"]')
end_date.clear()
end_date.send_keys("31/12/2018")
variable = driver.find_element_by_xpath("/html/body/div[2]/div/div[2]/div[1]/div/div/div[1]/div[3]/div[1]/div/div[2]/div[2]/table/tbody/tr[1]/td[2]/span")
y=variable.find_elements_by_class_name('k-input')

# driver.find_element_by_xpath(start_date).cl
# driver.find_element_by_xpath(start_date_elem).send_keys("01/01/1950")
# driver.find_element_by_xpath(end_date_elem).clear()
# driver.find_element_by_xpath(end_date_elem).send_keys("01/01/2019")
# elem = driver.find_element_by_name("q")
# elem.clear()
# elem.send_keys("pycon")
# elem.send_keys(Keys.RETURN)
# assert "No results found." not in driver.page_source
# driver.close()

