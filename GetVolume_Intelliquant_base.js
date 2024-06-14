// Parameters
var target_stock = [];
var ListingDate_stock = [];
var DelistingDate_stock = [];
var code_modified = [];
var target_stock_code =[];
var volume = [];
var volume_foreign = [];
var volume_institute = [];
var volume_retail = [];

var code_listed = 'A005930'; // 삼성전자. 2000년부터 계속 상장된 회사
var stock_listed;
var load_failure_list = [];  // 상폐일, 상장일이 기준일 조건을 충족하는데 종목 조회 실패한 데이터들의 인덱스
var DelistingDate_Error_list = [];	// 상폐일이 시뮬레이션 시작일보다 빠른 데이터들의 인덱스

var list_index = 0;
var NumOfStocks = 0;
var len_data = code.length;
var last_data_day;
var first_data_day = new Date('2000-04-03');

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
			var _stock = IQStock.getStock(code_modified[list_index]);                
            if (_stock != null)
            {
                target_stock[NumOfStocks] = _stock;
                target_stock_code[NumOfStocks] = code[list_index]; // target stock의 original 코드명 맵핑
                ListingDate_stock[NumOfStocks] = ListingDate[list_index]; // target stock의 상장일 맵핑
                DelistingDate_stock[NumOfStocks] = DelistingDate[list_index]; // target stock의 상폐일 맵핑                                                
                logger.info(target_stock_code[NumOfStocks] + ': ' + _stock.name + ', ' + NumOfStocks );
				NumOfStocks++;
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
                logger.info(target_stock_code[NumOfStocks] + ': ' + _stock.name + ', ' + NumOfStocks );
				NumOfStocks++;
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
	for (var i=0;  i<NumOfStocks; i++){
        if (DelistingDate_stock[i] >= today){ // 상폐일이 오늘 이후인 경우만 데이터 읽음
            volume[i] = target_stock[i].getTradingVolume();
            volume_foreign[i] = target_stock[i].getTradingVolumeFrgn();
            volume_institute[i] = target_stock[i].getTradingVolumeInst();
            volume_retail[i] = target_stock[i].getTradingVolumeIndv();
            logger.info(removeA(target_stock_code[i]) + ', V: ' + volume[i] + ', F: ' + volume_foreign[i] + ', I: ' + volume_institute[i] + ', R: ' + volume_retail[i]);
        }
    }   

    if (today >= FinalDate){ // 백테스트 마지막날    
        logger.info('list_index: ' + list_index);
        logger.info('NumOfStocks: ' +  NumOfStocks);
        logger.info('load_failure_list: [' + load_failure_list + ']');
        logger.info('DelistingDate_Error_list: [' + DelistingDate_Error_list + ']');        
	}    
}
