// Parameters
var target_stock = [];
var ListingDate_stock = [];
var DelistingDate_stock = [];
var code_modified = [];
var target_stock_code =[];
var revenue = [];
var GP = [];
var operating_income =[];
var net_profit = [];
var code_listed = 'A005930'; // 삼성전자. 2000년부터 계속 상장된 회사
var stock_listed;
var load_failure_list = [];  // 상폐일, 상장일이 기준일 조건을 충족하는데 종목 조회 실패한 데이터들의 인덱스
var DelistingDate_Error_list = [];	// 상폐일이 시뮬레이션 시작일보다 빠른 데이터들의 인덱스

var month_quarter_4 = 3; //4월. 전년도 4분기 데이터 업데이트
var month_quarter_1 = 5; //6월. 이번년도 1분기 데이터 업데이트
var month_quarter_2 = 8; //9월. 이번년도 2분기 데이터 업데이트
var month_quarter_3 = 11; //12월. 이번년도 3분기 데이터 업데이트
var last_quarter_4_rebal_year = 99; // 초기값을 이렇게 잡아야 2000년 1월 4일부터 돌릴때 2000년에 처음 동작한다.
var last_quarter_1_rebal_year = 99; 
var last_quarter_2_rebal_year = 99; 
var last_quarter_3_rebal_year = 99; 

var list_index = 0;
var NumOfStocks = 0;
var len_data = code.length;
var last_data_day;

function removeA(str) {
    return str.replace(/^A/, '');
}

function initialize() {
    for (var i = 0; i < len_data; i++) {
    	code_modified[i] = code[i].substring(0, code[i].length - 1) + '0'; // 코드명 마지막 글자 '0'으로 변경
	}
    for (var i = 0; i < len_data; i++) {
    	ListingDate[i].setHours(0, 0, 0, 0);
	}
    for (var i = 0; i < len_data; i++) {
    	DelistingDate[i].setHours(0, 0, 0, 0);
	}
    StartDate.setHours(0, 0, 0, 0);
    FinalDate.setHours(0, 0, 0, 0);
    stock_listed = IQStock.getStock(code_listed);
    
    while ((ListingDate[list_index] <= StartDate) && (list_index < len_data)){ // 더 최근 날짜가 더 크다
        if (DelistingDate[list_index] >= StartDate) {//DelistingDate 보다 기준일이 더 과거인지 확인
			var _stock = IQStock.getStock(code[list_index]);                
            if (_stock != null)
            {
                target_stock[NumOfStocks] = _stock;
                target_stock_code[NumOfStocks] = code[list_index]; // target stock의 original 코드명 맵핑
                ListingDate_stock[NumOfStocks] = ListingDate[list_index]; // target stock의 상장일 맵핑
                DelistingDate_stock[NumOfStocks] = DelistingDate[list_index]; // target stock의 상폐일 맵핑                                
                NumOfStocks++;
                logger.info('_stock: ' + _stock.name + ', ' + NumOfStocks );
            }
            else
            {
                load_failure_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준에 들어오는데 종목 조회 실패
            }            
		}
        else {
			DelistingDate_Error_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준 안 맞음
            logger.info('DelistingDate_Error: ' + code[list_index]);
        }
        list_index++;        
    }    
    // 상장일이 시뮬레이션 시작일보다 빠른 종목들 target_stock에 코드명 맵핑
}

function onDayClose(now){    
    var thisYear = now.getYear();
    var thisMonth = now.getMonth();
    var thisDate = now.getDate();
    var today = stock_listed.getDate();
	
	// today와 같거나 빠른 상장일의 종목들 등록
    while ((ListingDate[list_index] <= today) && (list_index < len_data)){ // 더 최근 날짜가 더 크다
        if (DelistingDate[list_index] >= StartDate) {//DelistingDate 보다 기준일이 더 과거인지 확인
			var _stock = IQStock.getStock(code_modified[list_index]);                
            if (_stock != null)
            {
                target_stock[NumOfStocks] = _stock;
                target_stock_code[NumOfStocks] = code[list_index]; // target stock의 original 코드명 맵핑
                ListingDate_stock[NumOfStocks] = ListingDate[list_index]; // target stock의 상장일 맵핑
                DelistingDate_stock[NumOfStocks] = DelistingDate[list_index]; // target stock의 상폐일 맵핑
                NumOfStocks++;
                logger.info(_stock.name);
            }
            else
            {
                load_failure_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준에 들어오는데 종목 조회 실패
            }            
		}
        else {
			DelistingDate_Error_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준에 안 맞음
        }
        list_index++;        
    }
	
	// 재무 정보 기록
	if( thisMonth == month_quarter_1 && thisDate>=1 && thisYear > last_quarter_1_rebal_year){ // 6월 첫째날
		for (var i=0;  i<NumOfStocks; i++){  
			revenue[i] = target_stock[i].getFundamentalRevenue(); // 매출액 [천원]
			GP[i] = target_stock[i].getFundamentalGrossProfit(); // 매출총이익 [천원]
			operating_income[i] = target_stock[i].getFundamentalOperatingIncome(); // 영업이익 [천원]
			net_profit[i] = target_stock[i].getFundamentalNetProfit(); // 당기순이익 [천원]
			logger.info(removeA(target_stock_code[i]) + ', RV: ' + revenue[i] + ', GP: ' + GP[i] + ', OI: ' + operating_income[i] + ', NP: ' + net_profit[i]);
		}
		last_quarter_1_rebal_year = thisYear;
        last_data_day = today;
	}
    else if( thisMonth == month_quarter_2 && thisDate>=1 && thisYear > last_quarter_2_rebal_year){ // 9월 첫째날
		for (var i=0;  i<NumOfStocks; i++){  
			revenue[i] = target_stock[i].getFundamentalRevenue(); // 매출액 [천원]
			GP[i] = target_stock[i].getFundamentalGrossProfit(); // 매출총이익 [천원]
			operating_income[i] = target_stock[i].getFundamentalOperatingIncome(); // 영업이익 [천원]
			net_profit[i] = target_stock[i].getFundamentalNetProfit(); // 당기순이익 [천원]
			logger.info(removeA(target_stock_code[i]) + ', RV: ' + revenue[i] + ', GP: ' + GP[i] + ', OI: ' + operating_income[i] + ', NP: ' + net_profit[i]);
		}
		last_quarter_2_rebal_year = thisYear;
        last_data_day = today;
	}
    else if( thisMonth == month_quarter_3 && thisDate>=1 && thisYear > last_quarter_3_rebal_year){ // 12월 첫째날
		for (var i=0;  i<NumOfStocks; i++){  
			revenue[i] = target_stock[i].getFundamentalRevenue(); // 매출액 [천원]
			GP[i] = target_stock[i].getFundamentalGrossProfit(); // 매출총이익 [천원]
			operating_income[i] = target_stock[i].getFundamentalOperatingIncome(); // 영업이익 [천원]
			net_profit[i] = target_stock[i].getFundamentalNetProfit(); // 당기순이익 [천원]
			logger.info(removeA(target_stock_code[i]) + ', RV: ' + revenue[i] + ', GP: ' + GP[i] + ', OI: ' + operating_income[i] + ', NP: ' + net_profit[i]);
		}
		last_quarter_3_rebal_year = thisYear;
        last_data_day = today;
	}  
    else if( thisMonth == month_quarter_4 && thisDate>=1 && thisYear > last_quarter_4_rebal_year){ // 4월 첫째날
		for (var i=0;  i<NumOfStocks; i++){  
			revenue[i] = target_stock[i].getFundamentalRevenue(); // 매출액 [천원]
			GP[i] = target_stock[i].getFundamentalGrossProfit(); // 매출총이익 [천원]
			operating_income[i] = target_stock[i].getFundamentalOperatingIncome(); // 영업이익 [천원]
			net_profit[i] = target_stock[i].getFundamentalNetProfit(); // 당기순이익 [천원]
			logger.info(removeA(target_stock_code[i]) + ', RV: ' + revenue[i] + ', GP: ' + GP[i] + ', OI: ' + operating_income[i] + ', NP: ' + net_profit[i]);
		}
		last_quarter_4_rebal_year = thisYear;
        last_data_day = today;
	}  

    if (today >= FinalDate){ // 백테스트 마지막날
        for (var i=0;  i<NumOfStocks; i++){
            if( ListingDate_stock[i]>last_data_day) // 상장일이 마지막 fiancial data 습득일보다 뒤인 경우
			logger.info(removeA(target_stock_code[i]) + ', RV: 0, GP: 0, OI: 0, NP: 0');
		}       
        logger.info('list_index: ' + list_index);
        logger.info('NumOfStocks: ' +  NumOfStocks);
        logger.info('load_failure_list: [' + load_failure_list + ']');
        logger.info('DelistingDate_Error_list: [' + DelistingDate_Error_list + ']');        
	}    
}
