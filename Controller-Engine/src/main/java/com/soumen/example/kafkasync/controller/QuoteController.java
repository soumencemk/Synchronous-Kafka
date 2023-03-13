package com.soumen.example.kafkasync.controller;

import com.soumen.example.kafkasync.model.Quote;
import com.soumen.example.kafkasync.service.QuoteService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/quote")
@RequiredArgsConstructor
public class QuoteController {
    private final QuoteService quoteService;

    @GetMapping("/random")
    public Quote getAQuote(){
        return quoteService.getRandomQuote();
    }
}
