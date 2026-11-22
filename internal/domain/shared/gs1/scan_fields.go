package gs1

import "time"

func setSSCC(s *Scan, v string) error {
	x, err := ParseSSCC(v)
	if err != nil {
		return wrapScan(err)
	}
	s.SSCC = x
	return nil
}

func setGTIN(s *Scan, v string) error {
	x, err := ParseGTIN(v)
	if err != nil {
		return wrapScan(err)
	}
	s.GTIN = x
	return nil
}

func setContentGTIN(s *Scan, v string) error {
	x, err := ParseGTIN(v)
	if err != nil {
		return wrapScan(err)
	}
	s.ContentGTIN = x
	return nil
}

func inSet82(v string) bool {
	for i := 0; i < len(v); i++ {
		if !inCharacterSet82(v[i]) {
			return false
		}
	}
	return true
}

func setLot(s *Scan, v string) error {
	if !inSet82(v) {
		return invalidScan("lot has a character outside the GS1 82 character set")
	}
	s.Lot = v
	return nil
}

func setSerial(s *Scan, v string) error {
	if !inSet82(v) {
		return invalidScan("serial has a character outside the GS1 82 character set")
	}
	s.Serial = v
	return nil
}

func setPurchaseOrder(s *Scan, v string) error {
	if !inSet82(v) {
		return invalidScan("purchase order has a character outside the GS1 82 character set")
	}
	s.PurchaseOrder = v
	return nil
}

func setShipToGLN(s *Scan, v string) error {
	x, err := ParseGLN(v)
	if err != nil {
		return wrapScan(err)
	}
	s.ShipToGLN = x
	return nil
}

func setShipForGLN(s *Scan, v string) error {
	x, err := ParseGLN(v)
	if err != nil {
		return wrapScan(err)
	}
	s.ShipForGLN = x
	return nil
}

func setCount(s *Scan, v string) error {
	if !allDigits(v) {
		return invalidScan("count must be digits")
	}
	n := 0
	for i := 0; i < len(v); i++ {
		n = n*10 + int(v[i]-'0')
	}
	if n < 1 {
		return invalidScan("count must be at least 1")
	}
	s.Count = n
	return nil
}

// parseDate reads YYMMDD. DD=00 (day not specified) becomes the last day of the month.
func parseDate(v string, year yearResolver) (Date, error) {
	if !allDigits(v) {
		return Date{}, invalidScan("date must be YYMMDD digits")
	}
	yy := int(v[0]-'0')*10 + int(v[1]-'0')
	mm := int(v[2]-'0')*10 + int(v[3]-'0')
	dd := int(v[4]-'0')*10 + int(v[5]-'0')
	if mm < 1 || mm > 12 {
		return Date{}, invalidScan("date month out of range")
	}
	y, month := year(yy), time.Month(mm)
	last := time.Date(y, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
	if dd == 0 {
		dd = last // GS1: day 00 means the last day of the month
	}
	if dd > last {
		return Date{}, invalidScan("date day out of range")
	}
	return Date{Year: y, Month: month, Day: dd}, nil
}

// inCharacterSet82 reports whether c is in the GS1 AI-encodable character set 82 (printable
// ASCII without space, # $ @ [ \ ] ^ ` { | } ~).
func inCharacterSet82(c byte) bool {
	// % & ' ( ) * + , - . / 0-9 : ; < = > ?
	if c >= '%' && c <= '?' {
		return true
	}
	if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' {
		return true
	}
	return c == '!' || c == '"' || c == '_'
}
