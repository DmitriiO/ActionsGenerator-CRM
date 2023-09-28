Sub CommandButton1_Click()

    'off default func'

    'Application.DisplayStatusBar = False'
    Application.StatusBar = "Îáíîâëåíèå äàííûõ..."
    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual

    Call Refresh_All_Data_Connections
    Call RefreshAllPivotTables
    Call CalcBook
    
    'on default func'
    Application.Calculation = xlCalculationAutomatic
    'Application.DisplayStatusBar = True'
    Application.StatusBar = ""
    Application.ScreenUpdating = True
    
    MsgBox "Äàííûå îáíîâëåíû!"

End Sub

Sub Refresh_All_Data_Connections()

    For Each objConnection In ThisWorkbook.Connections
        'Get current background-refresh value
        bBackground = objConnection.OLEDBConnection.BackgroundQuery

        'Temporarily disable background-refresh
        objConnection.OLEDBConnection.BackgroundQuery = False

        'Refresh this connection
        objConnection.Refresh

        'Set background-refresh value back to original value
        objConnection.OLEDBConnection.BackgroundQuery = bBackground
    
    Next

End Sub

Sub CalcBook()

    
    Dim wks As Worksheet
    
    For Each wks In ActiveWorkbook.Worksheets
    
        wks.Calculate
    
    Next
    
    Set wks = Nothing
    
End Sub

Sub RefreshAllPivotTables()

Dim PT As PivotTable
Dim WS As Worksheet

    For Each WS In ThisWorkbook.Worksheets

        For Each PT In WS.PivotTables
          
          PT.RefreshTable
        
        Next PT

    Next WS

End Sub
