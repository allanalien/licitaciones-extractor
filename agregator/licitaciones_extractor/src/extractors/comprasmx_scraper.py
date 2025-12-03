"""
ComprasMX web scraper for licitaciones extraction system.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from bs4 import BeautifulSoup
from datetime import date, datetime
from typing import List, Dict, Any, Optional
import time
import re

from .base_extractor import BaseExtractor, ExtractionResult


class ComprasMXScraper(BaseExtractor):
    """
    Web scraper for ComprasMX platform.

    This scraper navigates the Single Page Application (SPA) to extract
    tender information from the ComprasMX platform.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize ComprasMX scraper.

        Args:
            config: Configuration dictionary
        """
        super().__init__("comprasmx", config)

    def _setup_extractor(self):
        """Setup ComprasMX scraper specific configuration."""
        self.base_url = self.config.get(
            'base_url',
            'https://comprasmx.buengobierno.gob.mx/sitiopublico/#/'
        )
        self.timeout = self.config.get('timeout', 30)
        self.page_load_timeout = self.config.get('page_load_timeout', 60)
        self.headless = self.config.get('headless', True)
        self.max_pages = self.config.get('max_pages', 5)
        self.max_retries = self.config.get('max_retries', 3)

        self.driver = None

    def _setup_driver(self) -> webdriver.Chrome:
        """
        Setup Chrome WebDriver with appropriate options.

        Returns:
            Configured Chrome WebDriver instance
        """
        chrome_options = Options()

        if self.headless:
            chrome_options.add_argument('--headless')

        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(self.page_load_timeout)
            driver.implicitly_wait(10)
            return driver
        except Exception as e:
            self.logger.error(f"Failed to setup Chrome driver: {str(e)}")
            raise

    def extract_data(self, target_date: date, **kwargs) -> ExtractionResult:
        """
        Extract data from ComprasMX for a specific date.

        Args:
            target_date: Date to extract data for
            **kwargs: Additional parameters

        Returns:
            ExtractionResult with extracted data
        """
        self.logger.info(f"Starting ComprasMX scraping for {target_date}")

        try:
            # Setup driver
            self.driver = self._setup_driver()

            # Navigate to the site and extract data
            raw_records = self._scrape_tenders(target_date)

            self.logger.info(f"Scraped {len(raw_records)} records from ComprasMX")

            return ExtractionResult(
                success=True,
                records=raw_records,
                errors=[],
                source=self.source_name,
                extraction_date=datetime.utcnow(),
                metadata={
                    "target_date": target_date.isoformat(),
                    "total_records": len(raw_records),
                    "scraping_method": "selenium",
                    "base_url": self.base_url
                }
            )

        except Exception as e:
            error_msg = f"Error scraping ComprasMX data: {str(e)}"
            self.logger.error(error_msg)
            return ExtractionResult(
                success=False,
                records=[],
                errors=[error_msg],
                source=self.source_name,
                extraction_date=datetime.utcnow(),
                metadata={"target_date": target_date.isoformat()}
            )

        finally:
            # Always close the driver
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

    def _scrape_tenders(self, target_date: date) -> List[Dict[str, Any]]:
        """
        Optimized scraping method for ComprasMX daily table.

        Args:
            target_date: Date to scrape data for

        Returns:
            List of scraped records
        """
        all_records = []

        try:
            # Navigate to the main page
            self.driver.get(self.base_url)
            self.logger.info(f"Navigated to {self.base_url}")

            # Wait for SPA to load completely
            self.logger.info("Waiting for SPA to load...")
            wait = WebDriverWait(self.driver, 20)
            time.sleep(8)  # Allow dynamic content to load

            # Find the main data table using optimized selectors
            table_rows = self._find_table_rows()

            if not table_rows:
                self.logger.warning("No table rows found")
                return []

            self.logger.info(f"Found {len(table_rows)} table rows")

            # Extract data from each row
            for i, row in enumerate(table_rows):
                try:
                    row_data = self._extract_row_data(row, i)
                    if row_data:
                        all_records.append(row_data)
                except Exception as e:
                    self.logger.warning(f"Error processing row {i}: {str(e)}")
                    continue

            self.logger.info(f"Successfully extracted {len(all_records)} records")
            return all_records

        except Exception as e:
            self.logger.error(f"Error in _scrape_tenders: {str(e)}")
            raise

    def _find_table_rows(self) -> List:
        """
        Find table rows using multiple selector strategies.

        Returns:
            List of WebElement rows
        """
        # Optimized selectors based on testing
        table_selectors = [
            'table tbody tr',
            '.table tbody tr',
            'table tr',
            '.data-table tr',
            '[role="row"]',
            '.grid-row',
            '.table-row'
        ]

        for selector in table_selectors:
            try:
                rows = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if rows and len(rows) > 5:  # Minimum viable table size
                    self.logger.info(f"Found table with selector: {selector} ({len(rows)} rows)")
                    return rows
            except Exception as e:
                continue

        # Try alternative structures if no table found
        alt_selectors = [
            'div[class*="item"]',
            '.card',
            '.list-item',
            '[data-testid*="item"]'
        ]

        for selector in alt_selectors:
            try:
                items = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if len(items) > 5:
                    self.logger.info(f"Found alternative structure: {selector} ({len(items)} elements)")
                    return items
            except Exception as e:
                continue

        return []

    def _extract_row_data(self, row, row_index: int) -> Optional[Dict[str, Any]]:
        """
        Extract data from a single table row using optimized parsing.

        Args:
            row: WebElement representing the row
            row_index: Index of the row for tracking

        Returns:
            Dictionary with extracted data or None
        """
        try:
            # Get HTML content of the row
            row_html = row.get_attribute('outerHTML')

            # Use BeautifulSoup for robust parsing
            soup = BeautifulSoup(row_html, 'html.parser')

            # Extract text from all cells
            cells = soup.find_all(['td', 'th', 'div'])
            cell_texts = []

            for cell in cells:
                text = cell.get_text(strip=True)
                if text and len(text) > 2:  # Only meaningful text
                    cell_texts.append(text)

            if not cell_texts:
                return None

            # Create base record structure
            row_data = {
                'row_index': row_index,
                'extraction_timestamp': datetime.utcnow().isoformat(),
                'all_text': ' | '.join(cell_texts),
                'cell_count': len(cell_texts),
                'cells': cell_texts[:15]  # Store first 15 cells
            }

            # Intelligent field identification
            self._identify_fields(row_data, cell_texts)

            # Generate unique ID
            row_data['tender_id'] = self._generate_tender_id_from_cells(cell_texts, row_index)

            return row_data

        except Exception as e:
            self.logger.error(f"Error extracting row {row_index}: {str(e)}")
            return None

    def _identify_fields(self, row_data: Dict[str, Any], cell_texts: List[str]) -> None:
        """
        Intelligently identify fields in the cell data.

        Args:
            row_data: Dictionary to update with identified fields
            cell_texts: List of cell text values
        """
        for i, cell_text in enumerate(cell_texts[:10]):  # Check first 10 cells
            cell_lower = cell_text.lower()

            # Identify process type
            if any(keyword in cell_lower for keyword in ['licitación', 'concurso', 'adjudicación', 'invitación']):
                row_data['tipo_proceso'] = cell_text

            # Identify estimated value
            elif any(keyword in cell_lower for keyword in ['$', 'peso', 'mxn', 'precio']) or \
                 (cell_text.replace(',', '').replace('.', '').isdigit() and len(cell_text) > 4):
                row_data['valor_estimado'] = cell_text

            # Identify description (longer texts)
            elif len(cell_text) > 20 and any(keyword in cell_lower for keyword in
                    ['adquisición', 'servicio', 'obra', 'suministro', 'mantenimiento']):
                if 'descripcion' not in row_data:  # Only take the first long description
                    row_data['descripcion'] = cell_text[:200] + '...' if len(cell_text) > 200 else cell_text

            # Identify entity/institution
            elif any(keyword in cell_lower for keyword in ['secretaría', 'instituto', 'comisión', 'gobierno']):
                row_data['entidad'] = cell_text

            # Identify dates
            elif self._looks_like_date(cell_text):
                if 'fecha_apertura' not in row_data:
                    row_data['fecha_apertura'] = cell_text

            # Identify reference number (alphanumeric codes)
            elif self._looks_like_reference(cell_text) and 'numero_referencia' not in row_data:
                row_data['numero_referencia'] = cell_text

    def _looks_like_date(self, text: str) -> bool:
        """
        Check if text looks like a date.

        Args:
            text: Text to check

        Returns:
            True if text appears to be a date
        """
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{4}',
            r'\d{4}-\d{1,2}-\d{1,2}',
            r'\d{1,2}-\d{1,2}-\d{4}',
            r'\d{1,2}\s+\w+\s+\d{4}'
        ]

        for pattern in date_patterns:
            if re.search(pattern, text):
                return True
        return False

    def _looks_like_reference(self, text: str) -> bool:
        """
        Check if text looks like a reference number.

        Args:
            text: Text to check

        Returns:
            True if text appears to be a reference
        """
        # Check for common reference patterns
        if len(text) < 5 or len(text) > 50:
            return False

        # Must contain both letters and numbers
        has_letters = any(c.isalpha() for c in text)
        has_numbers = any(c.isdigit() for c in text)

        # Common reference patterns
        reference_patterns = [
            r'^[A-Z]{2,4}-\d+',
            r'\d{4}-[A-Z]+',
            r'[A-Z]+\d{6,}',
            r'\w+-\w+-\w+'
        ]

        if has_letters and has_numbers:
            for pattern in reference_patterns:
                if re.search(pattern, text.upper()):
                    return True

        return False

    def _generate_tender_id_from_cells(self, cell_texts: List[str], row_index: int) -> str:
        """
        Generate a unique tender ID from cell content.

        Args:
            cell_texts: List of cell texts
            row_index: Row index for fallback

        Returns:
            Generated tender ID
        """
        # Try to find a reference-like cell for ID
        for cell in cell_texts[:5]:  # Check first 5 cells
            if self._looks_like_reference(cell):
                return f"comprasmx_{cell.replace(' ', '_')}"

        # Fallback: use hash of first few cells
        content_hash = str(hash(' '.join(cell_texts[:3])))[-8:]
        return f"comprasmx_{content_hash}_{row_index}"
        self._wait_for_page_load()

        # Navigate to search/tender listing section
        self._navigate_to_tender_section()

        # Apply date filter if possible
        self._apply_date_filter(target_date)

        # Scrape pages
        page = 1
        while page <= self.max_pages:
            try:
                self.logger.info(f"Scraping page {page}")

                # Get current page data
                page_records = self._scrape_current_page()

                if not page_records:
                    self.logger.info(f"No records found on page {page}, stopping")
                    break

                all_records.extend(page_records)
                self.logger.info(f"Found {len(page_records)} records on page {page}")

                # Try to navigate to next page
                if not self._go_to_next_page():
                    self.logger.info("No more pages available")
                    break

                page += 1
                time.sleep(2)  # Delay between pages

            except Exception as e:
                self.logger.error(f"Error scraping page {page}: {str(e)}")
                break

        return all_records

    def _wait_for_page_load(self):
        """Wait for the SPA to load completely."""
        try:
            # Wait for common elements that indicate the page has loaded
            WebDriverWait(self.driver, self.timeout).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CLASS_NAME, "tender")),
                    EC.presence_of_element_located((By.CLASS_NAME, "licitacion")),
                    EC.presence_of_element_located((By.TAG_NAME, "table")),
                    EC.presence_of_element_located((By.CLASS_NAME, "search")),
                    EC.presence_of_element_located((By.CLASS_NAME, "content"))
                )
            )
            time.sleep(3)  # Additional wait for dynamic content
        except TimeoutException:
            self.logger.warning("Timeout waiting for page to load, continuing anyway")

    def _navigate_to_tender_section(self):
        """Navigate to the tender/licitaciones section."""
        try:
            # Look for common navigation elements
            nav_selectors = [
                "a[href*='licitacion']",
                "a[href*='tender']",
                "a[href*='buscar']",
                "a[href*='search']",
                ".nav-link:contains('Licitaciones')",
                ".menu-item:contains('Licitaciones')"
            ]

            for selector in nav_selectors:
                try:
                    if 'contains' in selector:
                        # Use XPath for text-based selection
                        xpath = f"//a[contains(text(), 'Licitaciones') or contains(text(), 'Buscar')]"
                        element = self.driver.find_element(By.XPATH, xpath)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)

                    element.click()
                    self.logger.info(f"Clicked navigation element: {selector}")
                    time.sleep(3)
                    return
                except NoSuchElementException:
                    continue

            self.logger.warning("Could not find tender navigation section, continuing with current page")

        except Exception as e:
            self.logger.warning(f"Error navigating to tender section: {str(e)}")

    def _apply_date_filter(self, target_date: date):
        """
        Apply date filter if available.

        Args:
            target_date: Date to filter for
        """
        try:
            # Look for date input fields
            date_selectors = [
                "input[type='date']",
                "input[name*='fecha']",
                "input[name*='date']",
                "input[placeholder*='fecha']",
                "input[placeholder*='date']"
            ]

            date_str = target_date.strftime('%Y-%m-%d')

            for selector in date_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        element.clear()
                        element.send_keys(date_str)
                        time.sleep(1)

                    if elements:
                        self.logger.info(f"Applied date filter: {date_str}")

                        # Look for search/filter button
                        self._click_search_button()
                        return

                except Exception as e:
                    continue

            self.logger.info("No date filter found or applied")

        except Exception as e:
            self.logger.warning(f"Error applying date filter: {str(e)}")

    def _click_search_button(self):
        """Click search/filter button."""
        search_selectors = [
            "button[type='submit']",
            "button:contains('Buscar')",
            "button:contains('Filtrar')",
            "button:contains('Search')",
            ".btn-search",
            ".search-btn",
            "input[type='submit']"
        ]

        for selector in search_selectors:
            try:
                if 'contains' in selector:
                    xpath = f"//button[contains(text(), 'Buscar') or contains(text(), 'Filtrar') or contains(text(), 'Search')]"
                    element = self.driver.find_element(By.XPATH, xpath)
                else:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)

                element.click()
                self.logger.info("Clicked search button")
                time.sleep(3)
                return

            except NoSuchElementException:
                continue

    def _scrape_current_page(self) -> List[Dict[str, Any]]:
        """
        Scrape tender data from the current page.

        Returns:
            List of tender records from current page
        """
        records = []

        try:
            # Get page source and parse with BeautifulSoup
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # Look for tender containers using various selectors
            tender_selectors = [
                '.tender-item',
                '.licitacion-item',
                'tr[class*="tender"]',
                'tr[class*="licitacion"]',
                '.card',
                '.procurement-item',
                'tbody tr'
            ]

            tender_elements = []
            for selector in tender_selectors:
                elements = soup.select(selector)
                if elements:
                    tender_elements = elements
                    self.logger.info(f"Found {len(elements)} tenders using selector: {selector}")
                    break

            if not tender_elements:
                # Fallback: look for any structured data
                tender_elements = soup.find_all(['tr', 'div'], class_=re.compile(r'(item|card|row)'))
                self.logger.warning(f"Using fallback selector, found {len(tender_elements)} potential records")

            # Extract data from each tender element
            for element in tender_elements:
                try:
                    record = self._extract_tender_data(element)
                    if record:
                        records.append(record)
                except Exception as e:
                    self.logger.error(f"Error extracting tender data: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"Error scraping current page: {str(e)}")

        return records

    def _extract_tender_data(self, element) -> Optional[Dict[str, Any]]:
        """
        Extract tender data from a single element.

        Args:
            element: BeautifulSoup element containing tender data

        Returns:
            Dictionary with tender data or None
        """
        try:
            # Extract title
            title = self._extract_text_by_selectors(element, [
                '.title', '.titulo', '.nombre', 'h3', 'h4', '.name',
                'td:nth-child(1)', 'td:nth-child(2)'
            ])

            # Extract description
            description = self._extract_text_by_selectors(element, [
                '.description', '.descripcion', '.detalle', '.detail',
                'td:nth-child(3)', 'td:nth-child(4)', '.content'
            ])

            # Extract entity
            entity = self._extract_text_by_selectors(element, [
                '.entity', '.entidad', '.dependencia', '.organismo',
                '.institution', 'td:contains("entidad")', '.agency'
            ])

            # Extract dates
            fecha_text = self._extract_text_by_selectors(element, [
                '.date', '.fecha', '.publication-date', 'td:contains("fecha")',
                '.created', '.published'
            ])

            # Extract amount
            amount_text = self._extract_text_by_selectors(element, [
                '.amount', '.monto', '.valor', '.budget', '.presupuesto',
                'td:contains("$")', 'td:contains("peso")'
            ])

            # Extract URL if available
            url = self._extract_url(element)

            # Skip if no meaningful title
            if not title or len(title.strip()) < 5:
                return None

            return {
                'titulo': title,
                'descripcion': description,
                'entidad': entity,
                'fecha_texto': fecha_text,
                'monto_texto': amount_text,
                'url_original': url,
                'pagina_origen': self.driver.current_url,
                'metodo_extraccion': 'scraping',
                'html_content': str(element)[:1000]  # Keep first 1000 chars for debugging
            }

        except Exception as e:
            self.logger.error(f"Error extracting data from element: {str(e)}")
            return None

    def _extract_text_by_selectors(self, element, selectors: List[str]) -> str:
        """
        Extract text using multiple selectors.

        Args:
            element: BeautifulSoup element to search in
            selectors: List of CSS selectors to try

        Returns:
            Extracted text or empty string
        """
        for selector in selectors:
            try:
                if ':contains(' in selector:
                    # Handle :contains() pseudo-selector manually
                    search_text = selector.split(':contains(')[1].split(')')[0].strip('"\'')
                    found_element = element.find(text=re.compile(search_text, re.I))
                    if found_element:
                        return found_element.get_text(strip=True) if hasattr(found_element, 'get_text') else str(found_element).strip()
                else:
                    found_element = element.select_one(selector)
                    if found_element:
                        return found_element.get_text(strip=True)
            except Exception:
                continue

        return ""

    def _extract_url(self, element) -> Optional[str]:
        """
        Extract URL from tender element.

        Args:
            element: BeautifulSoup element

        Returns:
            URL string or None
        """
        try:
            # Look for links
            link = element.find('a', href=True)
            if link:
                href = link['href']
                if href.startswith('http'):
                    return href
                else:
                    # Relative URL, make it absolute
                    base_domain = self.base_url.split('#')[0].rstrip('/')
                    return f"{base_domain}{href}"
        except Exception:
            pass

        return None

    def _go_to_next_page(self) -> bool:
        """
        Navigate to the next page.

        Returns:
            True if successfully navigated to next page, False otherwise
        """
        try:
            # Look for next page button
            next_selectors = [
                '.pagination .next',
                '.pagination .siguiente',
                'button:contains("Siguiente")',
                'button:contains("Next")',
                '.page-next',
                'a[aria-label="Next"]',
                '.pagination a:last-child'
            ]

            for selector in next_selectors:
                try:
                    if ':contains(' in selector:
                        xpath = f"//button[contains(text(), 'Siguiente') or contains(text(), 'Next')]"
                        element = self.driver.find_element(By.XPATH, xpath)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)

                    if element.is_enabled():
                        element.click()
                        time.sleep(3)  # Wait for page to load
                        return True

                except NoSuchElementException:
                    continue

            return False

        except Exception as e:
            self.logger.error(f"Error navigating to next page: {str(e)}")
            return False

    def normalize_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize ComprasMX scraped data to standard format.

        Args:
            raw_data: Raw scraped data

        Returns:
            List of normalized data dictionaries
        """
        normalized_data = []

        for record in raw_data:
            try:
                normalized_record = self._normalize_single_record(record)
                if normalized_record:
                    normalized_data.append(normalized_record)
            except Exception as e:
                self.logger.error(f"Error normalizing ComprasMX record: {str(e)}")
                continue

        return normalized_data

    def _normalize_single_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Normalize a single scraped record from optimized extraction.

        Args:
            record: Single raw record from optimized scraping

        Returns:
            Normalized record or None if invalid
        """
        try:
            # Use the already extracted tender_id
            tender_id = record.get('tender_id', f"comprasmx_{record.get('row_index', 0)}")

            # Extract fields from the optimized extraction
            titulo = self._clean_text(
                record.get('descripcion') or
                record.get('numero_referencia') or
                record.get('all_text', '')[:100] or
                "Licitación ComprasMX"
            )

            descripcion = self._clean_text(
                record.get('descripcion') or
                record.get('all_text', '')[:500] or
                ""
            )

            entidad = self._clean_text(
                record.get('entidad') or
                self._extract_entity_from_text(record.get('all_text', '')) or
                "ComprasMX"
            )

            # Handle dates
            fecha_catalogacion = None
            fecha_apertura = None

            if record.get('fecha_apertura'):
                fecha_apertura = self._normalize_date(record.get('fecha_apertura'))
                fecha_catalogacion = fecha_apertura
            else:
                # Try to extract from all text
                fecha_catalogacion = self._extract_date_from_text(record.get('all_text', ''))

            # Handle monetary values
            valor_estimado = None
            if record.get('valor_estimado'):
                valor_estimado = self._normalize_amount(record.get('valor_estimado'))
            else:
                valor_estimado = self._extract_amount_from_text(record.get('all_text', ''))

            # Process type
            tipo_licitacion = self._clean_text(
                record.get('tipo_proceso') or
                self._infer_tender_type_from_text(record.get('all_text', ''))
            )

            # Location defaults
            estado = "México"
            ciudad = self._extract_location_from_text(record.get('all_text', '')) or ""

            # URL construction
            url_original = self._construct_comprasmx_url(record)

            # Create normalized record
            normalized_record = {
                "tender_id": str(tender_id),
                "fuente": self.source_name,
                "titulo": titulo,
                "descripcion": descripcion,
                "entidad": entidad,
                "estado": estado,
                "ciudad": ciudad,
                "fecha_catalogacion": fecha_catalogacion,
                "fecha_apertura": fecha_apertura,
                "valor_estimado": valor_estimado,
                "tipo_licitacion": tipo_licitacion,
                "url_original": url_original,
                "metadata": {
                    "fuente_original": self.source_name,
                    "fecha_extraccion": datetime.utcnow().isoformat(),
                    "parametros_busqueda": {},
                    "datos_especificos": {
                        "comprasmx": {
                            "row_index": record.get('row_index'),
                            "cell_count": record.get('cell_count'),
                            "extraction_timestamp": record.get('extraction_timestamp'),
                            "numero_referencia": record.get('numero_referencia'),
                            "metodo_extraccion": "optimized_scraping",
                            "all_text_sample": record.get('all_text', '')[:200]
                        }
                    },
                    "calidad_datos": {
                        "completitud": self._calculate_completeness({
                            'titulo': titulo,
                            'descripcion': descripcion,
                            'entidad': entidad,
                            'fecha': fecha_catalogacion,
                            'valor': valor_estimado
                        }),
                        "confiabilidad": 0.8 if record.get('cell_count', 0) > 5 else 0.6
                    }
                }
            }

            # Create semantic text for embeddings
            normalized_record["texto_semantico"] = self._create_semantic_text(normalized_record)

            return normalized_record

        except Exception as e:
            self.logger.error(f"Error normalizing record {record.get('row_index', 'unknown')}: {str(e)}")
            return None

    def _extract_entity_from_text(self, text: str) -> str:
        """
        Extract entity/institution name from text.

        Args:
            text: Text to search in

        Returns:
            Extracted entity name or empty string
        """
        entity_keywords = [
            'secretaría',
            'instituto',
            'comisión',
            'gobierno',
            'municipio',
            'ayuntamiento',
            'semar',
            'pemex',
            'cfe',
            'imss',
            'issste'
        ]

        text_lower = text.lower()
        words = text.split()

        for i, word in enumerate(words):
            word_lower = word.lower()
            for keyword in entity_keywords:
                if keyword in word_lower:
                    # Return the word and potentially the next 1-2 words
                    entity_parts = words[i:i+3]
                    return ' '.join(entity_parts)[:50]

        return ""

    def _extract_date_from_text(self, text: str):
        """
        Extract date from text using patterns.

        Args:
            text: Text to search

        Returns:
            Normalized date or None
        """
        date_patterns = [
            r'(\d{1,2}/\d{1,2}/\d{4})',
            r'(\d{4}-\d{1,2}-\d{1,2})',
            r'(\d{1,2}-\d{1,2}-\d{4})',
            r'(\d{1,2}\s+\w+\s+\d{4})'
        ]

        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                date_str = match.group(1)
                return self._normalize_date(date_str)

        return None

    def _extract_amount_from_text(self, text: str):
        """
        Extract monetary amount from text.

        Args:
            text: Text to search

        Returns:
            Extracted amount or None
        """
        # Look for currency patterns
        amount_patterns = [
            r'\$[\d,]+\.?\d*',
            r'[\d,]+\.?\d*\s*pesos',
            r'[\d,]+\.?\d*\s*mxn',
            r'[\d,]{4,}'
        ]

        for pattern in amount_patterns:
            match = re.search(pattern, text.lower())
            if match:
                amount_str = match.group(0)
                return self._normalize_amount(amount_str)

        return None

    def _extract_location_from_text(self, text: str) -> str:
        """
        Extract location from text.

        Args:
            text: Text to search

        Returns:
            Location name or empty string
        """
        mexican_states = [
            'aguascalientes', 'baja california', 'bcs', 'campeche', 'chiapas',
            'chihuahua', 'cdmx', 'coahuila', 'colima', 'durango', 'guanajuato',
            'guerrero', 'hidalgo', 'jalisco', 'méxico', 'michoacán', 'morelos',
            'nayarit', 'nuevo león', 'oaxaca', 'puebla', 'querétaro',
            'quintana roo', 'san luis potosí', 'sinaloa', 'sonora', 'tabasco',
            'tamaulipas', 'tlaxcala', 'veracruz', 'yucatán', 'zacatecas'
        ]

        text_lower = text.lower()
        for state in mexican_states:
            if state in text_lower:
                return state.title()

        return ""

    def _infer_tender_type_from_text(self, text: str) -> str:
        """
        Infer tender type from text content.

        Args:
            text: Text to analyze

        Returns:
            Inferred tender type
        """
        text_lower = text.lower()

        if 'licitación pública' in text_lower:
            return 'Licitación Pública'
        elif 'invitación' in text_lower:
            return 'Invitación Restringida'
        elif 'adjudicación directa' in text_lower:
            return 'Adjudicación Directa'
        elif 'concurso' in text_lower:
            return 'Concurso'
        else:
            return 'Licitación'

    def _construct_comprasmx_url(self, record: Dict[str, Any]) -> Optional[str]:
        """
        Construct URL for ComprasMX tender.

        Args:
            record: Record data

        Returns:
            Constructed URL or None
        """
        # ComprasMX URLs are typically dynamic, return base URL
        return self.base_url

    def _calculate_completeness(self, fields: Dict[str, Any]) -> float:
        """
        Calculate data completeness score.

        Args:
            fields: Dictionary of fields to check

        Returns:
            Completeness score between 0 and 1
        """
        total_fields = len(fields)
        complete_fields = sum(1 for value in fields.values() if value)
        return complete_fields / total_fields if total_fields > 0 else 0.0

        return normalized_record

    def _parse_date_from_text(self, date_text: str) -> Optional[date]:
        """
        Parse date from text.

        Args:
            date_text: Text containing date

        Returns:
            Parsed date or None
        """
        if not date_text:
            return None

        # Extract date patterns
        import re
        date_patterns = [
            r'(\d{1,2}/\d{1,2}/\d{4})',
            r'(\d{4}-\d{1,2}-\d{1,2})',
            r'(\d{1,2}-\d{1,2}-\d{4})'
        ]

        for pattern in date_patterns:
            match = re.search(pattern, date_text)
            if match:
                date_str = match.group(1)
                return self._normalize_date(date_str)

        return None

    def _parse_amount_from_text(self, amount_text: str) -> Optional[float]:
        """
        Parse amount from text.

        Args:
            amount_text: Text containing amount

        Returns:
            Parsed amount or None
        """
        if not amount_text:
            return None

        # Extract numeric value
        import re
        # Remove currency symbols and clean up
        cleaned = re.sub(r'[^\d.,]', '', amount_text)

        return self._normalize_amount(cleaned)

    def _infer_tender_type(self, title: str, description: str) -> str:
        """
        Infer tender type from title and description.

        Args:
            title: Tender title
            description: Tender description

        Returns:
            Inferred tender type
        """
        text = f"{title} {description}".lower()

        if any(word in text for word in ['construcción', 'obra', 'infraestructura']):
            return "Obra Pública"
        elif any(word in text for word in ['servicios', 'consultoria', 'asesoría']):
            return "Servicios"
        elif any(word in text for word in ['suministro', 'adquisición', 'compra']):
            return "Suministros"
        elif any(word in text for word in ['tecnología', 'software', 'equipo']):
            return "Tecnología"
        else:
            return "General"

    def validate_data(self, normalized_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate normalized ComprasMX data.

        Args:
            normalized_data: Normalized data to validate

        Returns:
            List of valid data dictionaries
        """
        valid_data = []

        for record in normalized_data:
            validation_errors = self._validate_single_record(record)

            if not validation_errors:
                valid_data.append(record)
            else:
                self.logger.warning(f"ComprasMX record {record.get('tender_id')} failed validation: {validation_errors}")

        return valid_data

    def _validate_single_record(self, record: Dict[str, Any]) -> List[str]:
        """
        Validate a single normalized record.

        Args:
            record: Record to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Check required fields
        missing_fields = self._validate_required_fields(record)
        errors.extend(missing_fields)

        # ComprasMX specific validations
        if record.get('fuente') != 'comprasmx':
            errors.append("Record must be from comprasmx source")

        # Check minimum content length
        titulo = record.get('titulo', '')
        if len(titulo) < 5:
            errors.append("titulo must have at least 5 characters")

        # Validate dates if present
        if record.get('fecha_catalogacion') and not isinstance(record.get('fecha_catalogacion'), date):
            errors.append("fecha_catalogacion must be a date object")

        return errors

    def get_source_info(self) -> Dict[str, Any]:
        """
        Get information about the ComprasMX scraper.

        Returns:
            Dictionary with source information
        """
        base_info = super().get_source_info()
        base_info.update({
            "base_url": self.base_url,
            "scraping_method": "selenium",
            "headless_mode": self.headless,
            "timeout": self.timeout,
            "max_pages": self.max_pages,
            "geographic_scope": "México"
        })
        return base_info