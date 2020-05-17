import os
import requests
from contextlib import suppress
from time import sleep

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

# https://github.com/mozilla/geckodriver/releases
pwd = os.path.dirname(os.path.realpath(__file__))

class WebDriver:
    def __init__(self):
        self.driver = webdriver.Firefox(executable_path=os.path.join(pwd, "geckodriver"))

    def destroy(self):
        self.driver.quit()

    def visit(self, url):
        self.driver.get(url)

    def extract(self, url):
        sources = requests.get("https://cdn.jsdelivr.net/npm/sweetalert2@9").text
        injected_javascript = (sources + """
          function getPathTo(element) {
            if (element.tagName == 'HTML')
              return '/HTML[1]';
            if (element===document.body)
              return '/HTML[1]/BODY[1]';

            var ix= 0;
            var siblings= element.parentNode.childNodes;
            for (var i= 0; i<siblings.length; i++) {
              var sibling= siblings[i];
              if (sibling===element)
                return getPathTo(element.parentNode)+'/'+element.tagName+'['+(ix+1)+']';
              if (sibling.nodeType===1 && sibling.tagName===element.tagName)
                ix++;
            }
          }

          async function modal(event) {
            Swal.fire({
              title: 'Select annotation type',
              input: 'select',
              inputOptions: {
                click: 'click',
                title: 'title',
                download_url: 'download-url',
                spatial_extent: 'spatial-extent',
                spatial_resolution: 'spatial-resolution',
                temporal_resolution: 'temporal-resolution'
              },
              inputPlaceholder: 'Select a type',
              showCancelButton: true,
              cancelButtonText: 'Done'
            }).then((result) => {
              if (result.value) {
                var elems = document.getElementsByTagName("click-event");
                if (elems.length === 1) {
                  var elem = elems[0];
                } else {
                  var elem = document.createElement("click-event");
                  document.body.appendChild(elem);
                }

                elem.innerHTML += result.value + ':' + getPathTo(event.target) + ';';
                console.log(elem.innerHTML);

              } else if (result.dismiss === Swal.DismissReason.cancel) {
                var elem = document.createElement("dismiss-event");
                document.body.appendChild(elem);
              }
            });
          }

          document.body.addEventListener('click', function(event){
            name = event.target.className || event.target.parentElement.className
            if (event.ctrlKey && name.indexOf("swal2") < 0) {
              event.preventDefault();
              modal(event);
            }
          }, true);
        """)

        href_self = """
          // force all hrefs to open in current tab
          var links = document.getElementsByTagName('a');
          for (var i=0, len=links.length; i < len; i++) {
            links[i].target = '_self';
          };
        """

        # Navigate to the page and inject the JavaScript.
        self.driver.execute_script("window.open()")
        self.driver.switch_to_window(self.driver.window_handles[1])
        self.driver.get(url)
        self.driver.implicitly_wait(0)
        self.driver.execute_script(injected_javascript)

        # Wait until we find our events, created when an element is clicked by the user
        current_url = url
        while True:
            self.driver.execute_script(href_self)
            if self.driver.current_url != current_url: # Make sure our click handler is loaded
                sleep(0.5)
                self.driver.execute_script(injected_javascript)
                current_url = self.driver.current_url

            with suppress(NoSuchElementException):
                xpaths = self.driver.find_element_by_tag_name("click-event").text
                done = self.driver.find_element_by_tag_name("dismiss-event")

                if done:
                    break

            sleep(1)

        clicks = []
        results = {
            "access_url": (None ,url)
        }

        for xpath in xpaths[:-1].split(';'):
            elem_type, elem_path = xpath.split(':')

            if elem_type == "click":
                clicks.append(elem_path)
                self.driver.find_element_by_xpath(elem_path).click()
            else:
                elem = self.driver.find_element_by_xpath(elem_path)
                href = elem.get_attribute("href")
                results[elem_type] = (';'.join(clicks + [elem_path]), elem.text if not href else href)

        self.driver.close()
        self.driver.switch_to_window(self.driver.window_handles[0])

        return results


if __name__ == "__main__":
    driver = WebDriver()
    driver.extract("https://data.amsterdam.nl/datasets/R8T654t1DguJyg/openbare-sportplekken/")

