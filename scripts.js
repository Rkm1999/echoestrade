document.addEventListener('DOMContentLoaded', function() {
    // Global Variables
    const itemSelectorContainer = document.getElementById('item-selector-container');
    const chartCanvas = document.getElementById('priceChart');
    const newSMAPeriodInput = document.getElementById('newSMAPeriod');
    const addSMAButton = document.getElementById('addSMAButton');
    const activeSMAListDiv = document.getElementById('activeSMAList');
    const sidebarSearchInput = document.getElementById('sidebarSearch');
    const chartDisplayTitleElement = document.getElementById('chartDisplayTitle');

    let priceChart = null;
    let originalPriceData = [];
    let originalLabels = [];
    let activeSMAPeriods = [7]; // Initialize with a default array *first*.
    console.log('Initial activeSMAPeriods:', JSON.stringify(activeSMAPeriods));

    const smaColors = [
        'rgb(255, 99, 132)',  // Pink/Red
        'rgb(54, 162, 235)',  // Blue
        'rgb(255, 206, 86)',  // Yellow
        'rgb(153, 102, 255)', // Purple
        'rgb(255, 159, 64)',  // Orange
        'rgb(46, 204, 113)'   // Green
    ];

    function loadSMAPeriodsFromStorage() {
        let periodsToUse = [7]; // Default value for this function's scope
        const storedSMAPeriods = localStorage.getItem('activeSMAPeriods');
        console.log('Stored SMA periods from localStorage:', storedSMAPeriods);

        if (storedSMAPeriods) {
            try {
                const parsedPeriods = JSON.parse(storedSMAPeriods);
                console.log('Parsed SMA periods:', JSON.stringify(parsedPeriods));
                // Validate: must be an array, and if not empty, all elements must be positive numbers.
                if (Array.isArray(parsedPeriods)) {
                    if (parsedPeriods.length === 0) {
                        periodsToUse = []; // Valid empty array
                    } else if (parsedPeriods.every(p => typeof p === 'number' && p > 0)) {
                        periodsToUse = parsedPeriods; // Valid non-empty array
                    } else {
                        console.warn('Stored SMA periods data is invalid (not all positive numbers). Using default and resetting storage.');
                        localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
                    }
                } else {
                    console.warn('Stored SMA periods data is not an array. Using default and resetting storage.');
                    localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
                }
            } catch (e) {
                console.error('Error parsing stored SMA periods:', e, '. Using default and resetting storage.');
                localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
            }
        } else {
            console.log('No SMA periods found in localStorage. Using default and saving to storage.');
            localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
        }
        console.log('loadSMAPeriodsFromStorage will return:', JSON.stringify(periodsToUse));
        return periodsToUse;
    }

    activeSMAPeriods = loadSMAPeriodsFromStorage();
    console.log('Active SMA Periods after storage load attempt:', JSON.stringify(activeSMAPeriods));

    function saveSMAPeriods() {
        localStorage.setItem('activeSMAPeriods', JSON.stringify(activeSMAPeriods));
    }

    function fetchItemData() {
        fetch('item_data.json')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(jsonData => {
                itemSelectorContainer.innerHTML = '';
                buildSidebarNavigation(jsonData, itemSelectorContainer, 0);
            })
            .catch(error => {
                console.error('Error fetching item_data.json:', error);
                itemSelectorContainer.innerHTML = '<p>Error loading item data.</p>';
            });
    }

    function buildSidebarNavigation(data, parentElement, level = 0) {
        parentElement.innerHTML = '';

        const ul = document.createElement('ul');
        ul.className = `list-level-${level}`;
        ul.style.listStyleType = 'none';
        ul.style.paddingLeft = level > 0 ? '20px' : '0';

        for (const key in data) {
            const listItem = document.createElement('li');
            listItem.className = 'nav-item';

            const textElement = document.createElement('span');
            textElement.style.cursor = 'pointer';
            listItem.appendChild(textElement);

            if (typeof data[key] === 'string') {
                listItem.classList.add('item-leaf');
                textElement.textContent = key;
                listItem.dataset.csvPath = data[key];
                textElement.addEventListener('click', function() {
                    loadItemData(data[key], key);

                    const currentlySelected = document.querySelector('.nav-item.selected-leaf');
                    if (currentlySelected) {
                        currentlySelected.classList.remove('selected-leaf');
                    }
                    listItem.classList.add('selected-leaf');
                });
            } else if (typeof data[key] === 'object') {
                listItem.classList.add('item-category');
                textElement.textContent = '► ' + key;

                const childListContainer = document.createElement('div');
                childListContainer.className = 'child-list-container';
                childListContainer.style.display = 'none';
                listItem.appendChild(childListContainer);

                textElement.addEventListener('click', function(event) {
                    const isCurrentlyCollapsed = childListContainer.style.display === 'none';
                    if (isCurrentlyCollapsed) {
                        childListContainer.style.display = 'block';
                        textElement.textContent = '▼ ' + key;
                        if (childListContainer.innerHTML.trim() === '') {
                            buildSidebarNavigation(data[key], childListContainer, level + 1);
                        }
                    } else {
                        childListContainer.style.display = 'none';
                        textElement.textContent = '► ' + key;
                    }
                    event.stopPropagation();
                });
            }
            ul.appendChild(listItem);
        }
        parentElement.appendChild(ul);
    }


    function loadItemData(csvPath, itemName) {
        console.log("Loading data for:", csvPath, "Item Name:", itemName);
        fetch(csvPath)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status} for ${csvPath}`);
                }
                return response.text();
            })
            .then(csvData => {
                const lines = csvData.trim().split(/\r?\n/);
                if (lines.length <= 1) {
                    console.warn("CSV data has no data rows for", csvPath);
                    originalLabels = [];
                    originalPriceData = [];
                } else {
                    const dataRows = lines.slice(1);
                    const tempLabels = [];
                    const tempPriceData = [];
                    const priceIndex = 2;
                    const dateCreatedIndex = 5;

                    dataRows.forEach(row => {
                        const columns = row.split(',');
                        if (columns.length > Math.max(priceIndex, dateCreatedIndex)) {
                            const rawDate = columns[dateCreatedIndex];
                            tempLabels.push(rawDate.substring(0, 10));
                            const price = parseFloat(columns[priceIndex]);
                            tempPriceData.push(isNaN(price) ? null : price);
                        }
                    });
                    originalLabels = tempLabels;
                    originalPriceData = tempPriceData;
                }

                if (chartDisplayTitleElement && itemName) {
                    chartDisplayTitleElement.textContent = `${itemName} Price Over Time`;
                } else if (chartDisplayTitleElement) {
                    chartDisplayTitleElement.textContent = 'Price Over Time';
                }
                console.log('Loading item data. Current activeSMAPeriods before updateChartWithIndicators:', JSON.stringify(activeSMAPeriods));
                if (originalLabels.length === 0 && originalPriceData.length === 0) {
                    console.warn("No data parsed from CSV:", csvPath, "Clearing chart.");
                    if (priceChart) {
                        priceChart.data.labels = [];
                        priceChart.data.datasets = [];
                        priceChart.update();
                    } else {
                         initializeChart();
                    }
                } else {
                    updateChartWithIndicators();
                }
            })
            .catch(error => {
                console.error('Failed to load or process item data:', error);
                if (chartDisplayTitleElement) {
                    chartDisplayTitleElement.textContent = 'Failed to Load Item Data';
                }
                 originalLabels = [];
                 originalPriceData = [];
                 if (priceChart) {
                    priceChart.data.labels = [];
                    priceChart.data.datasets = [];
                    priceChart.update();
                 } else {
                    initializeChart();
                 }
            });
    }

    function calculateSMA(dates, prices, periodDays) {
        if (!dates || !prices || dates.length !== prices.length || periodDays <= 0) {
          return new Array(prices ? prices.length : 0).fill(null);
        }
        const smaValues = new Array(prices.length).fill(null);
        for (let i = 0; i < dates.length; i++) {
          const currentDateStr = dates[i];
          const currentDate = new Date(currentDateStr + "T00:00:00");
          const startDate = new Date(currentDate);
          startDate.setDate(currentDate.getDate() - (periodDays - 1));
          let sum = 0;
          let count = 0;
          for (let j = i; j >= 0; j--) {
            const loopDateStr = dates[j];
            const loopDate = new Date(loopDateStr + "T00:00:00");
            if (loopDate >= startDate && loopDate <= currentDate) {
              if (prices[j] !== null && !isNaN(prices[j])) {
                sum += prices[j];
                count++;
              }
            } else if (loopDate < startDate) {
              break;
            }
          }
          if (count > 0) {
            smaValues[i] = sum / count;
          }
        }
        return smaValues;
    }

    function renderActiveSMAList() {
        if (!Array.isArray(activeSMAPeriods)) {
            console.error('CRITICAL: activeSMAPeriods is not an array in renderActiveSMAList! Reverting to default [7].');
            activeSMAPeriods = [7];
            saveSMAPeriods();
        }
        if (!activeSMAListDiv) return;
        activeSMAListDiv.innerHTML = '';
        if (activeSMAPeriods.length === 0) {
          activeSMAListDiv.textContent = 'No active SMAs.';
          return;
        }
        activeSMAPeriods.forEach(period => {
          const itemDiv = document.createElement('div');
          itemDiv.classList.add('sma-item');
          const periodText = document.createElement('span');
          periodText.textContent = `SMA(${period})`;
          const removeButton = document.createElement('button');
          removeButton.textContent = 'Remove';
          removeButton.dataset.period = period;
          removeButton.addEventListener('click', function() {
            const periodToRemove = parseInt(this.dataset.period, 10);
            activeSMAPeriods = activeSMAPeriods.filter(p => p !== periodToRemove);
            saveSMAPeriods();
            renderActiveSMAList();
            updateChartWithIndicators();
          });
          itemDiv.appendChild(periodText);
          itemDiv.appendChild(removeButton);
          activeSMAListDiv.appendChild(itemDiv);
        });
    }

    if (addSMAButton) {
        addSMAButton.addEventListener('click', function() {
            if (!newSMAPeriodInput) return;
            const newPeriod = parseInt(newSMAPeriodInput.value, 10);
            if (isNaN(newPeriod) || newPeriod < 2 || newPeriod > 200) {
              alert('Please enter a valid SMA period (e.g., 2-200 days).');
              return;
            }
            if (activeSMAPeriods.includes(newPeriod)) {
              alert(`SMA(${newPeriod}) is already active.`);
              return;
            }
            if (activeSMAPeriods.length >= smaColors.length) {
                alert(`Maximum of ${smaColors.length} SMAs can be added.`);
                return;
            }
            activeSMAPeriods.push(newPeriod);
            activeSMAPeriods.sort((a, b) => a - b);
            saveSMAPeriods();
            renderActiveSMAList();
            updateChartWithIndicators();
        });
    }

    function initializeChart() {
        if (!chartCanvas) {
            console.error("Chart canvas not found!");
            return;
        }
        console.log('Initializing chart. Current activeSMAPeriods:', JSON.stringify(activeSMAPeriods));
        const ctx = chartCanvas.getContext('2d');
        if (!ctx) {
            console.error("Failed to get 2D context from canvas!");
            return;
        }

        priceChart = new Chart(ctx, {
          type: 'line',
          data: {
            labels: [],
            datasets: []
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              y: {
                beginAtZero: false,
                ticks: { callback: function(value) { return value.toLocaleString(); } }
              },
              x: {
                type: 'time',
                time: {
                    unit: 'day',
                    tooltipFormat: 'MMM d, yyyy',
                    displayFormats: { day: 'MMM d' }
                },
                ticks: { autoSkip: true, maxTicksLimit: 20 }
              }
            },
            plugins: {
              tooltip: {
                callbacks: {
                  label: function(context) {
                    let label = context.dataset.label || '';
                    if (label) { label += ': '; }
                    if (context.parsed.y !== null) { label += context.parsed.y.toLocaleString(); }
                    return label;
                  }
                }
              },
              legend: {
                position: 'top',
              }
            }
          }
        });
        renderActiveSMAList();
    }

    function updateChartWithIndicators() {
        if (!Array.isArray(activeSMAPeriods)) {
            console.error('CRITICAL: activeSMAPeriods is not an array in updateChartWithIndicators! Reverting to default [7].');
            activeSMAPeriods = [7];
            saveSMAPeriods();
        }
        if (!priceChart) {
            initializeChart();
            if (!priceChart) return;
        }

        priceChart.data.labels = originalLabels.slice();
        const datasets = [];

        if (originalPriceData.length > 0) {
            datasets.push({
              label: 'Item Price',
              data: originalPriceData.slice(),
              borderColor: 'rgb(0, 0, 0)',
              backgroundColor: 'rgba(0, 0, 0, 0.1)',
              tension: 0.1,
              borderWidth: 2,
              pointRadius: 2,
              pointHoverRadius: 4,
              order: 0
            });

            activeSMAPeriods.forEach((period, index) => {
                const smaData = calculateSMA(originalLabels, originalPriceData, period);
                const color = smaColors[index % smaColors.length];

                datasets.push({
                    label: `SMA(${period})`,
                    data: smaData,
                    borderColor: color,
                    fill: false,
                    tension: 0.1,
                    borderWidth: 1.5,
                    pointRadius: 0,
                    pointHoverRadius: 0,
                    order: index + 1
                });
            });
        } else {
            datasets.push({
                label: 'Item Price',
                data: [],
                borderColor: 'rgb(0,0,0)'
            });
        }

        priceChart.data.datasets = datasets;
        priceChart.update();
    }

    // Initial calls
    fetchItemData();
    initializeChart();

    if (chartDisplayTitleElement) {
        chartDisplayTitleElement.textContent = 'Select an Item to View Data';
    }

    function recursiveFilter(ulElement, term) {
        if (!ulElement) return false;

        let anyChildMatch = false;
        const listItems = Array.from(ulElement.children).filter(child => child.tagName === 'LI');

        for (const listItem of listItems) {
            let currentItemMatch = false;
            const textElement = listItem.querySelector('span');
            let itemText = '';
            if (textElement) {
                itemText = textElement.textContent.replace(/^[►▼]\s*/, '').toLowerCase();
                if (itemText.includes(term)) {
                    currentItemMatch = true;
                }
            }

            const childListContainer = listItem.querySelector('.child-list-container');
            let hasMatchingChildren = false;

            if (childListContainer) {
                const childUl = childListContainer.querySelector('ul');
                if (childUl && childUl.children.length > 0) {
                    if (recursiveFilter(childUl, term)) {
                        hasMatchingChildren = true;
                    }
                }
                if (currentItemMatch || hasMatchingChildren) {
                    currentItemMatch = true;
                    childListContainer.style.display = 'block';
                    if (textElement) textElement.textContent = '▼ ' + textElement.textContent.replace(/^[►▼]\s*/, '');
                } else {
                    childListContainer.style.display = 'none';
                    if (textElement) textElement.textContent = '► ' + textElement.textContent.replace(/^[►▼]\s*/, '');
                }
            }

            if (currentItemMatch) {
                listItem.style.display = "";
                anyChildMatch = true;
            } else {
                listItem.style.display = "none";
            }
        }
        return anyChildMatch;
    }

    function resetFilters() {
        const rootList = itemSelectorContainer.querySelector('ul.list-level-0');
        if (!rootList) return;

        const allListItems = rootList.getElementsByTagName('li');
        for (const listItem of allListItems) {
            listItem.style.display = "";
            const textElement = listItem.querySelector('span');
            const childListContainer = listItem.querySelector('.child-list-container');
            if (childListContainer) {
                childListContainer.style.display = 'none';
                if (textElement) textElement.textContent = '► ' + textElement.textContent.replace(/^[►▼]\s*/, '');
            }
        }
    }

    function filterSidebar(searchTerm) {
        const term = searchTerm.toLowerCase();
        const rootList = itemSelectorContainer.querySelector('ul.list-level-0');

        if (!rootList) {
            console.log("Root list not found for filtering.");
            return;
        }

        if (term === "") {
            resetFilters();
            return;
        }
        recursiveFilter(rootList, term);
    }

    if (sidebarSearchInput) {
        sidebarSearchInput.addEventListener('input', function() {
            filterSidebar(this.value);
        });
    }

    console.log("scripts.js loaded and DOMContentLoaded event fired.");
});
